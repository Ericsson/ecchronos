#!/usr/bin/env python3
#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
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
import subprocess
from cass_config import CassandraCluster
from ecc_config import EcchronosConfig
import time
import logging
import docker

from typing import Optional
from docker.models.containers import Container

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = docker.from_env()

MAX_CHECK = 10
STARTUP_WAIT_TIME = 10


class EcchronosFixture:
    """Test fixture for managing Cassandra cluster and ecChronos lifecycle"""

    def __init__(self):
        self.instances: list[Container] = []
        self._build_ecchronos_image()

    def _define_volumes(self, volume_mounts=None):
        if volume_mounts is None:
            volume_mounts = {}
        volumes = {}
        for item in volume_mounts.values():
            volumes[item["host"]] = {"bind": item["container"], "mode": "rw"}

        # Mount behave test files
        volumes[f"{global_vars.PROJECT_BUILD_DIRECTORY}/../src/test/behave"] = {
            "bind": f"{global_vars.CONTAINER_BASE_DIR}/behave",
            "mode": "ro",
        }
        return volumes

    def _build_ecchronos_image(self):
        logger.info("Building ecChronos image")
        client.images.build(
            path=global_vars.ROOT_DIR,
            dockerfile="ecchronos-binary/src/test/bin/Dockerfile",
            tag="ecchronos:latest",
            buildargs={"JAVA_VERSION": global_vars.JAVA_VERSION, "PROJECT_VERSION": global_vars.PROJECT_VERSION},
        )

    def start_ecchronos(
        self,
        suffix="cluster-wide",
        bind_port=8080,
        ecchronos_config=None,
        fixed_ipv4_address=global_vars.ECC_CONTAINER_IP,
        cassandra_network=None,
    ) -> None:
        """Start ecChronos service"""
        if cassandra_network is None:
            raise ValueError("cassandra_network is required and cannot be None")
        if ecchronos_config is None:
            ecchronos_config = EcchronosConfig()
        ecchronos_config.modify_configuration()
        volumes = self._define_volumes(ecchronos_config.container_mounts)
        try:
            logger.info(f"Starting ecChronos-agent-{suffix}")
            env = {"SERVER_ADDRESS": "0.0.0.0"}
            if global_vars.LOCAL != "true":
                client_cert = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/clientcert.crt"
                client_key = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/clientkey.pem"
                client_ca = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/serverca.crt"
                env["ECCTOOL_CERT_FILE"] = client_cert
                env["ECCTOOL_KEY_FILE"] = client_key
                env["ECCTOOL_CA_FILE"] = client_ca

            container = client.containers.run(
                image="ecchronos:latest",
                detach=True,
                volumes=volumes,
                name=f"ecchronos-agent-{suffix}",
                remove=False,
                ports={"8080/tcp": bind_port},
                stdout=True,
                stderr=True,
                environment=env,
            )
            self.instances.append(container)
            network = client.networks.get(cassandra_network)
            network.connect(container, ipv4_address=fixed_ipv4_address)

            time.sleep(STARTUP_WAIT_TIME)

        except Exception as e:
            logger.error(f"Failed to start ecChronos: {e}")
            raise

    def wait_for_ecchronos_ready(self, fixed_ipv4_address=global_vars.ECC_CONTAINER_IP, bind_port=8080) -> None:
        """Wait for ecChronos to be ready to accept requests"""
        url = (
            global_vars.BASE_URL_TLS.format(ip=fixed_ipv4_address)
            if global_vars.LOCAL != "true"
            else global_vars.BASE_URL.format(ip=fixed_ipv4_address)
        )
        curl_cmd = ["curl", "--silent", "--fail", "--head", "--output", "/dev/null", "--insecure", url]

        if global_vars.LOCAL != "true":
            curl_cmd += [
                "--cert",
                f"{global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt",
                "--key",
                f"{global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem",
                "--cacert",
                f"{global_vars.CERTIFICATE_DIRECTORY}/serverca.crt",
                "--resolve",
                f"localhost:{bind_port}:127.0.0.1",
            ]

        for attempt in range(MAX_CHECK + 1):
            try:
                result = subprocess.run(curl_cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info("ecChronos is ready")
                    return
            except subprocess.SubprocessError as e:
                logger.warning(f"Health check attempt {attempt + 1} failed: {e}")

            if attempt < MAX_CHECK:
                logger.info(f"Waiting for ecChronos... (attempt {attempt + 1}/{MAX_CHECK})")
                time.sleep(STARTUP_WAIT_TIME * 2)

        raise TimeoutError(f"ecChronos failed to become ready after {MAX_CHECK} attempts")

    def cleanup(self) -> None:
        """Cleanup all resources"""
        if self.instances:
            for ecchronos in self.instances:
                try:
                    ecchronos.stop()
                    inspect_cmd = [
                        "docker",
                        "inspect",
                        ecchronos.id,
                        "--format",
                        '{{ range .Mounts }}{{ if eq .Type "volume" }}{{ .Name }}{{ println }}{{ end }}{{ end }}',
                    ]

                    result = subprocess.run(inspect_cmd, capture_output=True, text=True)
                    if result.returncode == 0:
                        volumes = [v for v in result.stdout.splitlines() if v.strip()]
                        if volumes:
                            rm_cmd = ["docker", "volume", "rm", *volumes]
                            subprocess.run(rm_cmd)
                    subprocess.run(["docker", "rm", ecchronos.id], capture_output=True, text=True)
                except Exception as e:
                    logger.error(f"Error cleaning up container {ecchronos.id}: {e}")


@pytest.fixture(scope="session")
def test_environment():
    """Session-scoped fixture for test environment lifecycle"""
    fixture = EcchronosFixture()
    try:
        yield fixture
    finally:
        fixture.cleanup()


@pytest.fixture(scope="session")
def install_cassandra_cluster():
    logger.info(f"Creating cluster with version {global_vars.CASSANDRA_VERSION}")
    cassandra_cluster = CassandraCluster(global_vars.LOCAL)
    try:
        cassandra_cluster.create_cluster()
        yield cassandra_cluster
    finally:
        try:
            # Stop and remove extra node first
            cassandra_cluster.stop_extra_node()
            cassandra_cluster.stop_cluster()
        except Exception as e:
            logger.error(f"Error stopping Cassandra cluster during cleanup: {e}")


def build_behave_command(cassandra_address=global_vars.DEFAULT_INITIAL_CONTACT_POINT) -> list[str]:
    """Build behave command for execution inside container"""

    base_command = [
        "behave",
        "--define",
        f"ecctool={global_vars.CONTAINER_BASE_DIR}/bin/ecctool",
        "--define",
        f"cassandra_address={cassandra_address}",
        "--define",
        "cql_user=eccuser",
        "--define",
        "cql_password=eccpassword",
    ]

    if global_vars.LOCAL == "true":
        base_command.extend(["--define", "no_tls"])
    else:
        tls_options = [
            "--define",
            f"ecc_client_cert={global_vars.CONTAINER_CERTIFICATE_PATH}/clientcert.crt",
            "--define",
            f"ecc_client_key={global_vars.CONTAINER_CERTIFICATE_PATH}/clientkey.pem",
            "--define",
            f"ecc_client_ca={global_vars.CONTAINER_CERTIFICATE_PATH}/serverca.crt",
            "--define",
            f"cql_client_cert={global_vars.CONTAINER_CERTIFICATE_PATH}/cert.crt",
            "--define",
            f"cql_client_key={global_vars.CONTAINER_CERTIFICATE_PATH}/key.pem",
            "--define",
            f"cql_client_ca={global_vars.CONTAINER_CERTIFICATE_PATH}/ca.crt",
        ]
        base_command.extend(tls_options)

    return base_command


def run_ecctool_state(params, container_name="ecchronos-agent-cluster-wide"):
    return run_ecctool(["state"] + params, container_name)


def run_ecctool_state_nodes(container_name="ecchronos-agent-cluster-wide"):
    return run_ecctool_state(["nodes"], container_name)


def run_ecctool_run_repair(params, container_name="ecchronos-agent-cluster-wide"):
    return run_ecctool(["run-repair"] + params, container_name)


def run_ecctool_repairs(params, container_name="ecchronos-agent-cluster-wide"):
    return run_ecctool(["repairs"] + params, container_name)


def run_ecctool(params, container_name="ecchronos-agent-cluster-wide"):
    cmd = [f"{global_vars.CONTAINER_BASE_DIR}/bin/ecctool"] + params
    try:
        container = client.containers.get(container_name)
    except docker.errors.NotFound:
        raise RuntimeError(
            f"Container '{container_name}' not found. Ensure ecChronos is started before running ecctool."
        )
    exec_result = container.exec_run(cmd, stream=False)

    exit_code = exec_result.exit_code
    output = exec_result.output
    return output, exit_code


def assert_nodes_size_is_equal(out, expected_nodes):
    output_data = out.decode("ascii").lstrip().rstrip().split("\n")
    rows = output_data[3:-1]
    assert len(rows) == expected_nodes, f"Expected {expected_nodes} nodes, but found {len(rows)}"
