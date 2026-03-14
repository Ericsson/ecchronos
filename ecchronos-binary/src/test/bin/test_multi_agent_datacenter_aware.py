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
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import global_variables as global_vars
import pytest
from ecc_config import EcchronosConfig
from tenacity import retry, retry_if_result, stop_after_delay, wait_fixed

from conftest import (
    assert_nodes_size_is_equal,
    run_ecctool_repairs,
    run_ecctool_run_repair,
    run_ecctool_state_nodes,
)

logger = logging.getLogger(__name__)
barrier = Barrier(2)

ECC_INSTANCE_NAME_DC1 = "ecchronos-agent-dc1"
ECC_INSTANCE_NAME_DC2 = "ecchronos-agent-dc2"

# Keep schedule values aligned across DCs so contention is deterministic in CI.
SCHEDULE_INTERVAL_TIME = 1
SCHEDULE_INTERVAL_UNIT = "minutes"
SCHEDULE_INITIAL_DELAY_TIME = 1
SCHEDULE_INITIAL_DELAY_UNIT = "minutes"


def run_repair(container, params):
    barrier.wait()
    return run_ecctool_run_repair(params, container)


def verify_repair_completed(container, params):
    return run_ecctool_repairs(params, container)


@pytest.mark.dependency(name="test_install_cassandra_cluster")
def test_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(4)


@pytest.mark.dependency(name="test_install_ecchronos_dc1", depends=["test_install_cassandra_cluster"])
def test_install_ecchronos_dc1(install_cassandra_cluster, test_environment):
    dcs = [{"name": global_vars.DC1}]
    ecchronos_config = EcchronosConfig(
        datacenter_aware=dcs,
        instance_name=ECC_INSTANCE_NAME_DC1,
        schedule_interval_time=SCHEDULE_INTERVAL_TIME,
        schedule_interval_unit=SCHEDULE_INTERVAL_UNIT,
        schedule_initial_delay_time=SCHEDULE_INITIAL_DELAY_TIME,
        schedule_initial_delay_unit=SCHEDULE_INITIAL_DELAY_UNIT,
    )
    test_environment.start_ecchronos(
        suffix="dc1",
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=ecchronos_config,
    )
    test_environment.wait_for_ecchronos_ready()
    out, _ = run_ecctool_state_nodes(ECC_INSTANCE_NAME_DC1)
    assert_nodes_size_is_equal(out, 2)


@pytest.mark.dependency(name="test_install_ecchronos_dc2", depends=["test_install_cassandra_cluster"])
def test_install_ecchronos_dc2(install_cassandra_cluster, test_environment):
    dcs = [{"name": global_vars.DC2}]
    ecchronos_config = EcchronosConfig(
        datacenter_aware=dcs,
        instance_name=ECC_INSTANCE_NAME_DC2,
        local_dc=global_vars.DC2,
        initial_contact_point=global_vars.DEFAULT_SEED_IP_DC2,
        schedule_interval_time=SCHEDULE_INTERVAL_TIME,
        schedule_interval_unit=SCHEDULE_INTERVAL_UNIT,
        schedule_initial_delay_time=SCHEDULE_INITIAL_DELAY_TIME,
        schedule_initial_delay_unit=SCHEDULE_INITIAL_DELAY_UNIT,
    )
    test_environment.start_ecchronos(
        suffix="dc2",
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=ecchronos_config,
        bind_port=8081,
        fixed_ipv4_address="172.29.0.8",
    )
    test_environment.wait_for_ecchronos_ready(fixed_ipv4_address="172.29.0.8", bind_port=8081)
    out, _ = run_ecctool_state_nodes(ECC_INSTANCE_NAME_DC2)
    assert_nodes_size_is_equal(out, 2)


@pytest.mark.dependency(
    name="test_lock_concurrency",
    depends=["test_install_cassandra_cluster", "test_install_ecchronos_dc1", "test_install_ecchronos_dc2"],
)
def test_lock_concurrency(install_cassandra_cluster, test_environment):
    logger.info("Running parallel on demand jobs inside instances")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_ecc1_dc1 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC1, ["-k", "test", "--all"])
            future_ecc1_dc2 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC2, ["-k", "test", "--all"])
            future_ecc2_dc1 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC1, ["-k", "test2", "--all"])
            future_ecc2_dc2 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC2, ["-k", "test2", "--all"])

            _, exit_code_ecc1_dc1 = future_ecc1_dc1.result()
            _, exit_code_ecc1_dc2 = future_ecc1_dc2.result()
            _, exit_code_ecc2_dc1 = future_ecc2_dc1.result()
            _, exit_code_ecc2_dc2 = future_ecc2_dc2.result()

            logger.info(
                "On Demand Jobs created with exit code: "
                f"{ECC_INSTANCE_NAME_DC1}: {(exit_code_ecc1_dc1, exit_code_ecc2_dc1)}, "
                f"{ECC_INSTANCE_NAME_DC2}: {(exit_code_ecc1_dc2, exit_code_ecc2_dc2)}"
            )

            if any(
                exit_code != 0
                for exit_code in [exit_code_ecc1_dc1, exit_code_ecc1_dc2, exit_code_ecc2_dc1, exit_code_ecc2_dc2]
            ):
                logger.error("Fail to create on demand jobs")
                pytest.fail("Fail to create on demand jobs")

            try:
                wait_for_repairs_completion_on_demand(executor)
            except Exception as e:
                pytest.fail(f"Repairs did not complete within 5 minutes: {e}")

    except Exception as e:
        logger.error(f"Failed to run behave tests: {e}")
        pytest.fail(f"Failed to run behave tests: {e}")


@pytest.mark.dependency(
    name="test_lock_concurrency_scheduled",
    depends=["test_install_cassandra_cluster", "test_install_ecchronos_dc1", "test_install_ecchronos_dc2"],
)
def test_lock_concurrency_scheduled(install_cassandra_cluster, test_environment):
    logger.info("Waiting for scheduled repair jobs to run in both instances")

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            try:
                wait_for_repairs_completion_scheduled(executor)
            except Exception as e:
                pytest.fail(f"Scheduled repairs did not complete within 10 minutes: {e}")

    except Exception as e:
        logger.error(f"Failed to run scheduled repair tests: {e}")
        pytest.fail(f"Failed to run scheduled repair tests: {e}")


def handle_repair_output(output_data):
    output_data = output_data.decode("ascii").lstrip().rstrip().split("\n")
    rows = output_data[3:-2]
    return [row.strip().strip("|").strip() for row in rows]


def all_repairs_completed(statuses):
    """Check if all repair statuses are COMPLETED"""
    return all(status == "COMPLETED" for status in statuses)


@retry(stop=stop_after_delay(300), wait=wait_fixed(10), retry=retry_if_result(lambda x: not x))
def wait_for_repairs_completion_on_demand(executor):
    """Wait for all repairs to complete in both datacenters (on-demand timeout ~5min)."""
    return _wait_for_repairs_completion_common(executor)


@retry(stop=stop_after_delay(600), wait=wait_fixed(10), retry=retry_if_result(lambda x: not x))
def wait_for_repairs_completion_scheduled(executor):
    """Wait for all repairs to complete in both datacenters (scheduled timeout ~10min)."""
    return _wait_for_repairs_completion_common(executor)


def _wait_for_repairs_completion_common(executor):
    # Do not limit results, otherwise we may only inspect a subset of repairs
    # and get false positives/false negatives in CI.
    params = []

    future_ecc_dc1 = executor.submit(verify_repair_completed, ECC_INSTANCE_NAME_DC1, params)
    future_ecc_dc2 = executor.submit(verify_repair_completed, ECC_INSTANCE_NAME_DC2, params)
    output_ecc_dc1, _ = future_ecc_dc1.result()
    output_ecc_dc2, _ = future_ecc_dc2.result()

    ecc_dc1_statuses = handle_repair_output(output_ecc_dc1)
    ecc_dc2_statuses = handle_repair_output(output_ecc_dc2)

    dc1_completed = all_repairs_completed(ecc_dc1_statuses)
    dc2_completed = all_repairs_completed(ecc_dc2_statuses)

    if dc1_completed and dc2_completed:
        logger.info("All repairs completed successfully")
        return True

    logger.info(
        "Waiting for repairs... "
        f"DC1: {sum(1 for s in ecc_dc1_statuses if s == 'COMPLETED')}/{len(ecc_dc1_statuses)}, "
        f"DC2: {sum(1 for s in ecc_dc2_statuses if s == 'COMPLETED')}/{len(ecc_dc2_statuses)}"
    )
    return False
