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


class TestFixture:
    """Test fixture for managing Cassandra cluster and ecChronos lifecycle"""

    def __init__(self):
        self.cassandra_cluster: Optional[CassandraCluster] = None
        self.ecchronos: Optional[Container] = None
        self.is_setup = False
        self.ecc_config = None
        self.volumes = {}

    def setup(self) -> CassandraCluster:
        """Setup test environment with Cassandra cluster and ecChronos configuration"""
        try:
            logger.info(f"Creating cluster with version {global_vars.CASSANDRA_VERSION}")
            self.cassandra_cluster = CassandraCluster(global_vars.LOCAL)
            self.cassandra_cluster.create_cluster()

            logger.info("Configuring ecChronos")
            self.ecc_config = EcchronosConfig(context=self.cassandra_cluster)
            self.ecc_config.modify_configuration()

            self.is_setup = True
            return self.cassandra_cluster

        except Exception as e:
            logger.error(f"Failed to setup test environment: {e}")
            self.cleanup()
            raise

    def _define_volumes(self):
        for item in self.ecc_config.container_mounts.values():
            self.volumes[item["host"]] = {"bind": item["container"], "mode": "rw"}

    def _build_ecchronos_image(self):
        logger.info("Building ecChronos image")
        client.images.build(
            path=global_vars.ROOT_DIR,
            dockerfile="ecchronos-binary/src/test/bin/Dockerfile",
            tag="ecchronos:latest",
            buildargs={"JAVA_VERSION": global_vars.JAVA_VERSION, "PROJECT_VERSION": global_vars.PROJECT_VERSION},
        )

    def start_ecchronos(self) -> None:
        """Start ecChronos service"""
        if not self.is_setup:
            raise RuntimeError("Test fixture not setup. Call setup() first.")

        try:
            self._define_volumes()
            self._build_ecchronos_image()
            logger.info("Starting ecChronos")
            container = client.containers.run(
                image="ecchronos:latest",
                detach=True,
                volumes=self.volumes,
                name="ecchronos-agent",
                remove=False,
                ports={"8080/tcp": 8080},
                stdout=True,
                stderr=True,
            )

            network = client.networks.get(self.cassandra_cluster.network)
            network.connect(container, ipv4_address=global_vars.ECC_CONTAINER_IP)
            self.ecchronos = container

            time.sleep(STARTUP_WAIT_TIME)

        except Exception as e:
            logger.error(f"Failed to start ecChronos: {e}")
            raise

    def wait_for_ecchronos_ready(self) -> None:
        """Wait for ecChronos to be ready to accept requests"""
        url = global_vars.BASE_URL_TLS if global_vars.LOCAL != "true" else global_vars.BASE_URL
        curl_cmd = ["curl", "--silent", "--fail", "--head", "--output", "/dev/null", url]

        if global_vars.LOCAL != "true":
            curl_cmd += [
                "--cert",
                f"{global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt",
                "--key",
                f"{global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem",
                "--cacert",
                f"{global_vars.CERTIFICATE_DIRECTORY}/serverca.crt",
            ]

        for attempt in range(MAX_CHECK + 1):
            try:
                result = subprocess.run(curl_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
        try:
            if self.ecchronos is not None:
                self.ecchronos.stop()
                inspect_cmd = [
                    "docker",
                    "inspect",
                    self.ecchronos.id,
                    "--format",
                    '{{ range .Mounts }}{{ if eq .Type "volume" }}{{ .Name }}{{ println }}{{ end }}{{ end }}',
                ]

                result = subprocess.run(inspect_cmd, capture_output=True, text=True, check=True)

                volumes = [v for v in result.stdout.splitlines() if v.strip()]

                if volumes:
                    rm_cmd = ["docker", "volume", "rm", *volumes]
                    subprocess.run(rm_cmd, check=True)
                subprocess.run(["docker", "rm", self.ecchronos.id], capture_output=True, text=True, check=True)
        except Exception as e:
            logger.error(f"Error stopping ecChronos during cleanup: {e}")

        if self.cassandra_cluster:
            try:
                # Stop and remove extra node first
                self.cassandra_cluster.stop_extra_node()
                self.cassandra_cluster.stop_cluster()
            except Exception as e:
                logger.error(f"Error stopping Cassandra cluster during cleanup: {e}")


@pytest.fixture(scope="session")
def test_environment():
    """Session-scoped fixture for test environment lifecycle"""
    fixture = TestFixture()
    try:
        fixture.setup()
        fixture.start_ecchronos()
        fixture.wait_for_ecchronos_ready()
        yield fixture.cassandra_cluster
    finally:
        fixture.cleanup()


def build_behave_command(cassandra_cluster: CassandraCluster, ecchronos_container: Container) -> list[str]:
    """Build behave command based on configuration"""
    base_command = [
        "behave",
        "--define",
        f"ecctool=docker exec ecchronos-agent {global_vars.CONTAINER_BASE_DIR}/bin/ecctool",
        "--define",
        f"cassandra_address={cassandra_cluster.cassandra_ip}",
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
            f"ecc_client_cert={global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt",
            "--define",
            f"ecc_client_key={global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem",
            "--define",
            f"ecc_client_ca={global_vars.CERTIFICATE_DIRECTORY}/serverca.crt",
            "--define",
            f"cql_client_cert={global_vars.CERTIFICATE_DIRECTORY}/cert.crt",
            "--define",
            f"cql_client_key={global_vars.CERTIFICATE_DIRECTORY}/key.pem",
            "--define",
            f"cql_client_ca={global_vars.CERTIFICATE_DIRECTORY}/ca.crt",
        ]
        base_command.extend(tls_options)

    return base_command


def run_ecctool_state(params):
    return run_ecctool(["state"] + params)


def run_ecctool_state_nodes():
    return run_ecctool_state(["nodes"])


def run_ecctool(params):
    cmd = [f"{global_vars.BASE_DIR}/bin/ecctool"] + params
    env = {}
    if global_vars.LOCAL != "true":
        client_cert = f"{global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt"
        client_key = f"{global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem"
        client_ca = f"{global_vars.CERTIFICATE_DIRECTORY}/serverca.crt"
        env = {"ECCTOOL_CERT_FILE": client_cert, "ECCTOOL_KEY_FILE": client_key, "ECCTOOL_CA_FILE": client_ca}
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )  # pylint: disable=consider-using-with
    out, err = proc.communicate()
    return out, err


def assert_nodes_size_is_equal(out, expected_nodes):
    output_data = out.decode("ascii").lstrip().rstrip().split("\n")
    rows = output_data[3:-1]
    assert len(rows) == expected_nodes, f"Expected {expected_nodes} nodes, but found {len(rows)}"
