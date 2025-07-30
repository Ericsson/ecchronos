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
from testcontainers.compose import DockerCompose
import docker
import os
from datetime import datetime, timedelta
from time import sleep
import logging
DEFAULT_WAIT_TIME_IN_SECS = 10

COMPOSE_FILE_NAME = "docker-compose.yml"
CASSANDRA_SEED_DC1_RC1_ND1 = "cassandra-seed-dc1-rack1-node1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraCluster:
    def __init__(self, local):
        self.local = local
        os.environ["CERTIFICATE_DIRECTORY"] = global_vars.CERTIFICATE_DIRECTORY
        self.cassandra_compose = DockerCompose(
            global_vars.CASSANDRA_DOCKER_COMPOSE_FILE_PATH,
            build=True, compose_file_name="docker-compose.yml",
            wait=False)


    def create_cluster(self):
        self.cassandra_compose.start()
        self._set_env()
        self._wait_for_nodes_to_be_up(4, 15000)
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


    @staticmethod
    def _create_cert_path():
        if not os.path.exists(global_vars.CASSANDRA_CERT_PATH):
            os.makedirs(global_vars.CASSANDRA_CERT_PATH)


    @staticmethod
    def _get_container_ip(container_id, network_name=None):
        client = docker.from_env()
        container = client.containers.get(container_id)
        networks = container.attrs['NetworkSettings']['Networks']

        if network_name:
            return networks[network_name]['IPAddress']
        else:
            return next(iter(networks.values()))['IPAddress']


    def _get_node_count(self):
        listCertificates = ["ls", "/etc/certificates"]

        stdoutListCertificates = self.cassandra_compose.exec_in_container(
            service_name=CASSANDRA_SEED_DC1_RC1_ND1, command=listCertificates)[0]
        
        logger.info(f"stdoutListCertificates: {stdoutListCertificates}")
        


        command = ["nodetool", "--ssl", "-u", "cassandra", "-pw", "cassandra", "status"]
        stdout = self.cassandra_compose.exec_in_container(
            service_name=CASSANDRA_SEED_DC1_RC1_ND1, command=command)[0]
        return stdout.split("UN", -1).__len__() - 1


    def _wait_for_nodes_to_be_up(self, expected_nodes, max_wait_time_in_millis):
        start_time = datetime.now()
        max_wait_time = timedelta(milliseconds=max_wait_time_in_millis)

        while start_time + max_wait_time > datetime.now():
            try:
                sleep(DEFAULT_WAIT_TIME_IN_SECS)
                if self._get_node_count() == expected_nodes:
                    return
            except Exception:
                # ignore and retry
                continue
        logging.info("dormindoooo")
        from time import sleep
        sleep(60)
        raise TimeoutError(f"Nodes did not go up after {max_wait_time_in_millis}ms")


    def _setup_db(self):
        command = ["sh", "-c", "/etc/cassandra/setup_db.sh"]
        self.cassandra_compose.exec_in_container(
                service_name=CASSANDRA_SEED_DC1_RC1_ND1,
                command=command
            )


    def stop_cluster(self):
        self.cassandra_compose.stop()
