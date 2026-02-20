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

import pytest
import global_variables as global_vars
import logging
from conftest import run_ecctool_state_nodes, assert_nodes_size_is_equal, run_ecctool_run_repair, run_ecctool_repairs
from ecc_config import EcchronosConfig
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from tenacity import retry, stop_after_delay, wait_fixed, retry_if_result

logger = logging.getLogger(__name__)
barrier = Barrier(2)

ECC_INSTANCE_NAME_DC1 = "ecchronos-agent-dc1"
ECC_INSTANCE_NAME_DC2 = "ecchronos-agent-dc2"


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
    ecchronos_config = EcchronosConfig(datacenter_aware=dcs, instance_name=ECC_INSTANCE_NAME_DC1)
    test_environment.start_ecchronos(
        suffix="dc1", cassandra_network=install_cassandra_cluster.network, ecchronos_config=ecchronos_config
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
            future_ecc_dc1 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC1, ["--all"])
            future_ecc_dc2 = executor.submit(run_repair, ECC_INSTANCE_NAME_DC2, ["--all"])

            _, exit_code_ecc_dc1 = future_ecc_dc1.result()
            _, exit_code_ecc_dc2 = future_ecc_dc2.result()

            logger.info(
                f"On Demand Jobs created with exit code: ecchronos-agent-dc1: {exit_code_ecc_dc1}, ecchronos-agent-dc2: {exit_code_ecc_dc2}"
            )

            if exit_code_ecc_dc1 != 0 and exit_code_ecc_dc2 != 0:
                logger.error("Fail to create on demand jobs")
                pytest.fail("Fail to create on demand jobs")

            # Wait for jobs to be finished in both instances within a time limit
            try:
                wait_for_repairs_completion(executor)
            except Exception as e:
                logger.error(f"Timeout: Repairs did not complete within 5 minutes")
                pytest.fail(f"Repairs did not complete within 5 minutes: {e}")

    except Exception as e:
        logger.error(f"Failed to run behave tests: {e}")
        pytest.fail(f"Failed to run behave tests: {e}")


def handle_repair_output(output_data):
    output_data = output_data.decode("ascii").lstrip().rstrip().split("\n")
    rows = output_data[3:-2]
    # Clean each row: remove pipes and strip whitespace
    return [row.strip().strip("|").strip() for row in rows]


def all_repairs_completed(statuses):
    """Check if all repair statuses are COMPLETED"""
    return all(status == "COMPLETED" for status in statuses)


@retry(stop=stop_after_delay(600), wait=wait_fixed(10), retry=retry_if_result(lambda x: not x))
def wait_for_repairs_completion(executor):
    """Wait for all repairs to complete in both datacenters"""
    params = ["-c", "4"]
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
        f"Waiting for repairs... DC1: {sum(1 for s in ecc_dc1_statuses if s == 'COMPLETED')}/{len(ecc_dc1_statuses)}, DC2: {sum(1 for s in ecc_dc2_statuses if s == 'COMPLETED')}/{len(ecc_dc2_statuses)}"
    )
    return False
