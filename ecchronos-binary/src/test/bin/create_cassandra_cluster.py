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

from __future__ import print_function

from testcontainers.compose import DockerCompose
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../.."))
CASSANDRA_CLUSTER_COMPOSE_FILE_PATH = os.path.join(PROJECT_ROOT, "cassandra-test-image/src/main/docker")
CASSANDRA_SEED_NODE_NAME = "cassandra-seed-dc1-rack1-node1"
CASSANDRA_NODE_NAME = "cassandra-node-dc1-rack1-node2"

def _create_cassandra_cluster():
    cassandra_cluster = DockerCompose(CASSANDRA_CLUSTER_COMPOSE_FILE_PATH, pull=True)
    try:
        cassandra_cluster.start()
        _define_env(cassandra_cluster=cassandra_cluster)
    except FileNotFoundError:
        print("Failed to initialize Cassandra Cluster")

def _define_env(cassandra_cluster):
    os.environ["CASSANDRA_SEED_IP"] = cassandra_cluster.get_service_host(CASSANDRA_SEED_NODE_NAME)
    os.environ["CASSANDRA_NODE_IP"] = cassandra_cluster.get_service_host(CASSANDRA_NODE_NAME)

if __name__ == "__main__":
    _create_cassandra_cluster()