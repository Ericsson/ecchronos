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
import logging
from conftest import run_ecctool_state_nodes, assert_nodes_size_is_equal

logger = logging.getLogger(__name__)

DEFAULT_WAIT_TIME_IN_SECS = 120


@pytest.mark.dependency(name="test_install_cassandra_cluster")
def test_install_cassandra_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(4)


@pytest.mark.dependency(name="test_install_ecchronos", depends=["test_install_cassandra_cluster"])
def test_install_ecchronos(install_cassandra_cluster, test_environment):
    test_environment.start_ecchronos(cassandra_network=install_cassandra_cluster.network)
    test_environment.wait_for_ecchronos_ready()
    out, _ = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 4)


@pytest.mark.dependency(name="test_add_node", depends=["test_install_ecchronos"])
def test_add_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.add_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(5, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(5)
    out, _ = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 5)


@pytest.mark.dependency(name="test_remove_node", depends=["test_add_node"])
def test_remove_node(install_cassandra_cluster, test_environment):
    install_cassandra_cluster.stop_extra_node()
    install_cassandra_cluster._wait_for_nodes_to_be_up(4, DEFAULT_WAIT_TIME_IN_SECS * 1000)
    assert install_cassandra_cluster.verify_node_count(4)
    out, _ = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 4)
