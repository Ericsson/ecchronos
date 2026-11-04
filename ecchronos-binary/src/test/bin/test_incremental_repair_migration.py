#!/usr/bin/env python3
#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import time

import pytest
from tenacity import retry, retry_if_result, stop_after_delay, wait_fixed

import incremental_repair_lib as lib
from ecc_config import EcchronosConfig

logger = logging.getLogger(__name__)

KEYSPACE = "repair_migration"
TABLE = "data"

EXPECTED_NODES = 4
# datacenter1:2, datacenter2:1 -> schedule present on all four nodes.
EXPECTED_SCHEDULES = 4

# Short interval so a cycle fires within each phase's window.
REPAIR_INTERVAL_MINUTES = 2
CYCLE_TIMEOUT_SECONDS = 900
POLL_INTERVAL_SECONDS = 10
SCHEDULE_DISCOVERY_TIMEOUT_SECONDS = 300

# The migration agent always runs on one fixed slot; each phase just stops the previous
# container and recreates it here with the new repair_type (that is the whole point of the
# migration - a restart with changed config on the same address). We reuse the default
# 8080/172.29.0.7 slot and, in phase 1, first stop any agent another suite left running in
# the same pytest session (test_environment is session-scoped) so this slot is free.
AGENT_BIND_PORT = 8080
AGENT_IP = "172.29.0.7"


def _config_for(repair_type):
    """Config that makes repair_type both the ecc.yml default and the per-table type."""
    return EcchronosConfig(
        repair_type=repair_type.lower(),
        schedule_keyspaces=lib.repair_schedule_keyspaces(KEYSPACE, TABLE, repair_type, REPAIR_INTERVAL_MINUTES),
    )


def _schedules():
    return lib.schedules_for(KEYSPACE, TABLE, container_name=_current_container_name())


# --- agent lifecycle ------------------------------------------------------------

# Name of the agent container the current phase is querying. start_ecchronos names the
# container "ecchronos-agent-<suffix>", and ecctool must run inside that specific container
# (the shared helpers otherwise default to the 'cluster-wide' agent, which this suite does
# not run). _recreate_agent updates this for each phase.
_CURRENT_SUFFIX = {"value": None}


def _current_container_name():
    suffix = _CURRENT_SUFFIX["value"]
    if suffix is None:
        raise RuntimeError("No migration agent has been started yet.")
    return f"ecchronos-agent-{suffix}"


def _stop_all_agents(test_environment):
    """Stop and remove every tracked agent so the fixed slot (IP/port) is free to reuse.

    Called before (re)creating the migration agent. It clears any agent still tracked in the
    session-scoped fixture - including one a previous suite left running - so recreating on
    the same address does not hit "port already allocated".
    """
    stopped = False
    for container in list(test_environment.instances):
        try:
            container.stop()
            container.remove()
        except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
            logger.warning("Could not stop agent %s: %s", getattr(container, "name", "?"), exc)
        test_environment.instances.remove(container)
        stopped = True
    # Let the ring drop the retired heartbeat before a new agent reuses the IP.
    if stopped:
        time.sleep(5)


def _recreate_agent(install_cassandra_cluster, test_environment, suffix, repair_type):
    """Stop the previous agent(s) and recreate one with repair_type on the fixed slot.

    This is the migration/rollback primitive: a restart with changed configuration on the
    same IP/port. Each phase calls it to move the whole instance to the next repair type.
    Records the new container name so subsequent ecctool queries target this agent.
    """
    _stop_all_agents(test_environment)
    test_environment.start_ecchronos(
        suffix=suffix,
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=_config_for(repair_type),
        bind_port=AGENT_BIND_PORT,
        fixed_ipv4_address=AGENT_IP,
    )
    _CURRENT_SUFFIX["value"] = suffix
    test_environment.wait_for_ecchronos_ready(fixed_ipv4_address=AGENT_IP, bind_port=AGENT_BIND_PORT)


@retry(
    stop=stop_after_delay(SCHEDULE_DISCOVERY_TIMEOUT_SECONDS),
    wait=wait_fixed(POLL_INTERVAL_SECONDS),
    retry=retry_if_result(lambda ok: not ok),
)
def _wait_for_schedules_discovered(expected_type):
    """Wait until all schedules are discovered AND report the expected repair type."""
    schedules = _schedules()
    if len(schedules) < EXPECTED_SCHEDULES:
        logger.info("Waiting for %s schedules (have %s)", EXPECTED_SCHEDULES, len(schedules))
        return False
    types = {s["repair_type"] for s in schedules}
    if types != {expected_type}:
        logger.info("Waiting for schedules to report %s (currently %s)", expected_type, types)
        return False
    return True


def _run_cycle_to_completion(cluster, offset, expected_type):
    """Write+flush fresh data and wait for a NEW repair cycle to COMPLETE under expected_type.

    Captures the lastRepaired baseline, then waits until lastRepaired advances past it with
    every schedule healthy (COMPLETED/ON_TIME), fully repaired, and still reporting
    expected_type - i.e. a fresh cycle genuinely ran and finished under the configured type.
    """
    baseline = lib.max_last_repaired(_schedules())
    lib.write_data(cluster, KEYSPACE, TABLE, rows=500, offset=offset)
    cluster.flush_all_nodes(keyspace=KEYSPACE)

    @retry(
        stop=stop_after_delay(CYCLE_TIMEOUT_SECONDS),
        wait=wait_fixed(POLL_INTERVAL_SECONDS),
        retry=retry_if_result(lambda done: not done),
    )
    def _new_cycle_completed():
        schedules = _schedules()
        if len(schedules) < EXPECTED_SCHEDULES:
            return False
        lib.assert_all_repair_type(schedules, expected_type, "schedule", KEYSPACE, TABLE)
        statuses = {s["status"] for s in schedules}
        assert "BLOCKED" not in statuses, f"{KEYSPACE}.{TABLE} schedule BLOCKED under {expected_type}"
        last_repaired = lib.max_last_repaired(schedules)
        ratio = lib.max_repaired_ratio(schedules)
        logger.info(
            "%s %s.%s: lastRepaired %s->%s ratio=%.2f statuses=%s",
            expected_type,
            KEYSPACE,
            TABLE,
            baseline,
            last_repaired,
            ratio,
            statuses,
        )
        healthy = all(s["status"] in lib.HEALTHY_STATUSES for s in schedules)
        return healthy and ratio >= 1.0 and last_repaired > baseline

    assert (
        _new_cycle_completed()
    ), f"No new {expected_type} repair cycle completed for {KEYSPACE}.{TABLE} within the timeout"


# ==================================================================================
# Bring-up
# ==================================================================================


@pytest.mark.dependency(name="mig_install_cluster")
def test_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(EXPECTED_NODES)
    lib.create_schema(install_cassandra_cluster, KEYSPACE, TABLE)


# ==================================================================================
# Phase 1: full (VNODE) baseline
# ==================================================================================


@pytest.mark.dependency(name="mig_phase_vnode", depends=["mig_install_cluster"])
def test_phase1_vnode_cycle_completes(install_cassandra_cluster, test_environment):
    # Phase 1 also clears any agent a prior suite left on the fixed slot in this session.
    _recreate_agent(install_cassandra_cluster, test_environment, "migration-vnode", lib.REPAIR_TYPE_VNODE)
    assert _wait_for_schedules_discovered(lib.REPAIR_TYPE_VNODE), "Agent did not come up as VNODE"
    _run_cycle_to_completion(install_cassandra_cluster, offset=0, expected_type=lib.REPAIR_TYPE_VNODE)


# ==================================================================================
# Phase 2: switch to INCREMENTAL (UC1) - restart agent, run a cycle to completion
# ==================================================================================


@pytest.mark.dependency(name="mig_phase_incremental", depends=["mig_phase_vnode"])
def test_phase2_switch_to_incremental_cycle_completes(install_cassandra_cluster, test_environment):
    _recreate_agent(install_cassandra_cluster, test_environment, "migration-incremental", lib.REPAIR_TYPE_INCREMENTAL)
    assert _wait_for_schedules_discovered(
        lib.REPAIR_TYPE_INCREMENTAL
    ), "ecChronos did not come up as INCREMENTAL after the switch (UC1)"
    _run_cycle_to_completion(install_cassandra_cluster, offset=10000, expected_type=lib.REPAIR_TYPE_INCREMENTAL)


# ==================================================================================
# Phase 3: rollback to VNODE (UC2) - restart agent, run a cycle to completion
# ==================================================================================


@pytest.mark.dependency(name="mig_phase_rollback", depends=["mig_phase_incremental"])
def test_phase3_rollback_to_vnode_cycle_completes(install_cassandra_cluster, test_environment):
    _recreate_agent(install_cassandra_cluster, test_environment, "migration-rollback", lib.REPAIR_TYPE_VNODE)
    assert _wait_for_schedules_discovered(
        lib.REPAIR_TYPE_VNODE
    ), "ecChronos did not roll back to VNODE after the switch (UC2)"
    _run_cycle_to_completion(install_cassandra_cluster, offset=20000, expected_type=lib.REPAIR_TYPE_VNODE)
