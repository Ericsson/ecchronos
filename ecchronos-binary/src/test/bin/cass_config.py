#!/usr/bin/env python3
# vi: syntax=python
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

import global_variables as global_vars
from testcontainers.compose import DockerCompose, ComposeContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from typing import Optional
import docker
import os
from datetime import datetime, timedelta
from time import sleep
import logging
import subprocess
import json

DEFAULT_WAIT_TIME_IN_SECS = 60

COMPOSE_FILE_NAME = "docker-compose.yml"
CASSANDRA_SEED_DC1_RC1_ND1 = "cassandra-seed-dc1-rack1-node1"
ALTER_SYSTEM_AUTH_CQL = "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 2};"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = docker.from_env()


class CassandraCluster:
    def __init__(self, local):
        self.cassandra_compose: Optional[DockerCompose] = None
        self.node: Optional[ComposeContainer] = None
        self._extra_node: str = None
        self.local = local
        os.environ["CERTIFICATE_DIRECTORY"] = global_vars.CERTIFICATE_DIRECTORY
        os.environ["CASSANDRA_VERSION"] = global_vars.CASSANDRA_VERSION
        os.environ["JOLOKIA"] = global_vars.JOLOKIA_ENABLED
        os.environ["PEM_ENABLED"] = global_vars.PEM_ENABLED
        os.environ["DOCKER_BUILDKIT"] = "1"
        self.cassandra_compose = DockerCompose(
            global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH,
            build=True,
            compose_file_name="docker-compose.yml",
            wait=False,
        )

    def create_cluster(self):
        try:
            self.cassandra_compose.start()
        except Exception as e:
            print(f"Error creating cluster: {e}")
            self.stop_cluster()
            raise e
        self._set_env()
        self._wait_for_nodes_to_be_up(4, DEFAULT_WAIT_TIME_IN_SECS * 1000)
        self._modify_system_auth_keyspace()
        self._run_full_repair()
        self._setup_db()

    def _set_env(self):
        try:
            self.node = self.cassandra_compose.get_container(CASSANDRA_SEED_DC1_RC1_ND1)
            self.container_id = self.node.ID
            self.cassandra_ip = self._get_container_ip(self.container_id)
            self.cassandra_native_port = 9042
            self.cassandra_jmx_port = 7199
            self._create_cert_path()
        except Exception as e:
            print(f"Error setting env: {e}")
            self.stop_cluster()
            raise e

    def add_node(self):
        client.images.build(
            path=global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH,
            dockerfile=f"{global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH}/Dockerfile",
            tag="cassandra-node3:latest",
            buildargs={"CASSANDRA_VERSION": global_vars.CASSANDRA_VERSION},
        )

        container_name = "cassandra-node-dc1-rack1-node3"
        rackdc_file = os.path.join(
            global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH, "cassandra-rackdc-dc1-rack1.properties"
        )
        cert_dir = global_vars.CERTIFICATE_DIRECTORY

        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format={{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}",
                self.container_id,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        network_name = result.stdout.strip()

        cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--network",
            network_name,
            "-e",
            "CASSANDRA_CLUSTER_NAME=cassandra-cluster",
            "-e",
            "CASSANDRA_DC=datacenter1",
            "-e",
            "CASSANDRA_RACK=rack1",
            "-e",
            "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
            "-e",
            "CASSANDRA_SEEDS=cassandra-seed-dc1-rack1-node1,cassandra-seed-dc2-rack1-node1,cassandra-node-dc1-rack1-node2,cassandra-node-dc2-rack1-node2",
            "-e",
            "CASSANDRA_PASSWORD_SEEDER=no",
            "-e",
            "CASSANDRA_PASSWORD=cassandra",
            "-e",
            "MAX_HEAP_SIZE=3G",
            "-e",
            "HEAP_NEWSIZE=400M",
            "-e",
            "LOCAL_JMX=no",
            "-e",
            f"JOLOKIA={global_vars.JOLOKIA_ENABLED}",
            "-e",
            f"PEM_ENABLED={global_vars.PEM_ENABLED}",
            "-e",
            "JVM_EXTRA_OPTS=-Dcom.sun.management.jmxremote.authenticate=false "
            "-Dcassandra.superuser_setup_delay_ms=0 "
            "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
            "-Dcassandra.ring_delay_ms=30000 "
            "-Dcassandra.write_survey=false",
            "-v",
            "cassandra-node3-data:/var/lib/cassandra",
            "-v",
            f"{rackdc_file}:/etc/cassandra/cassandra-rackdc.properties",
            "-v",
            f"{cert_dir}:/etc/certificates",
            "cassandra-node3:latest",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        new_container_id = result.stdout.strip()
        self._extra_node = new_container_id

    @staticmethod
    def _create_cert_path():
        if not os.path.exists(global_vars.CASSANDRA_CERT_PATH):
            os.makedirs(global_vars.CASSANDRA_CERT_PATH)

    @staticmethod
    def _get_container_ip(container_id, network_name=None):
        client = docker.from_env()
        container = client.containers.get(container_id)
        networks = container.attrs["NetworkSettings"]["Networks"]

        if network_name:
            return networks[network_name]["IPAddress"]
        else:
            return next(iter(networks.values()))["IPAddress"]

    def _get_node_count(self):
        if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
            command = ["docker", "exec", self.container_id, "sh", "-c", "~/.cassandra/nodetool-status-ssl.sh"]
        else:
            command = ["docker", "exec", self.container_id, "bash", "-c", "nodetool -u cassandra -pw cassandra status"]

        process = subprocess.run(
            command,
            check=True,
            timeout=DEFAULT_WAIT_TIME_IN_SECS,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return process.stdout.split("UN").__len__() - 1

    def verify_node_count(self, expected_nodes):
        try:
            if self._get_node_count() == expected_nodes:
                return True
            else:
                return False
        except Exception as e:
            print(f"Error verifying node count: {e}")
            return False

    def _wait_for_nodes_to_be_up(self, expected_nodes, max_wait_time_in_millis):
        start_time = datetime.now()
        max_wait_time = timedelta(milliseconds=max_wait_time_in_millis)

        while start_time + max_wait_time > datetime.now():
            try:
                sleep(5)
                if self._get_node_count() == expected_nodes:
                    return
            except Exception:
                # ignore and retry
                continue
        raise TimeoutError(f"Nodes did not go up after {max_wait_time_in_millis}ms")

    def _setup_db(self):
        command = ["docker", "exec", self.container_id, "bash", "/etc/cassandra/setup_db.sh"]

        subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Verify ecchronos keyspace exists before proceeding
        self._verify_keyspace_exists("ecchronos")
        logger.info("Database setup completed and verified")

    def _verify_keyspace_exists(self, keyspace_name):
        """Verify that a keyspace exists and is available on all nodes"""
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                command = ["docker", "exec", self.container_id, "cqlsh", "-e", f"DESCRIBE KEYSPACE {keyspace_name};"]
                result = subprocess.run(
                    command, timeout=10, encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                if result.returncode == 0 and keyspace_name in result.stdout:
                    logger.info(f"Keyspace {keyspace_name} verified on attempt {attempt + 1}")
                    return
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to verify keyspace {keyspace_name} failed: {e}")

            sleep(2)

        raise TimeoutError(f"Keyspace {keyspace_name} not available after {max_attempts} attempts")

    def _modify_system_auth_keyspace(self):
        logger.info("Changing system_auth replication strategy")
        command = ["docker", "exec", self.container_id, "cqlsh", "-e", f"{ALTER_SYSTEM_AUTH_CQL}"]
        subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _run_full_repair(self):
        logger.info("Running Full Repair")
        if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
            command = [
                "docker",
                "exec",
                self.container_id,
                "bash",
                "-c",
                "nodetool -ssl -u cassandra -pw cassandra repair --full",
            ]
        else:
            command = ["docker", "exec", self.container_id, "bash", "-c", "nodetool -u cassandra -pw cassandra repair --full"]

        subprocess.run(
            command,
            timeout=DEFAULT_WAIT_TIME_IN_SECS * 3,
            encoding="utf-8",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def stop_cluster(self):
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                f"{global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH}/docker-compose.yml",
                "down",
                "--volumes",
                "--remove-orphans",
                "--rmi",
                "all",
            ]
        )

    def stop_extra_node(self):
        try:
            if self._extra_node != None:
                if global_vars.LOCAL != "true" and global_vars.PEM_ENABLED != "true":
                    command = [
                        "docker",
                        "exec",
                        self._extra_node,
                        "bash",
                        "-c",
                        "nodetool -u cassandra -pw cassandra --ssl decommission",
                    ]
                else:
                    command = [
                        "docker",
                        "exec",
                        self.container_id,
                        "bash",
                        "-c",
                        "nodetool -u cassandra -pw cassandra decommission",
                    ]
                subprocess.run(command, capture_output=True, text=True, check=True)
                subprocess.run(["docker", "stop", self._extra_node], capture_output=True, text=True, check=True)
                subprocess.run(["docker", "rm", self._extra_node], capture_output=True, text=True, check=True)
                subprocess.run(
                    ["docker", "volume", "rm", "cassandra-node3-data"], capture_output=True, text=True, check=True
                )
                self._extra_node = None
        except Exception as e:
            print(f"Error removing extra node: {e}")
