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
from time import sleep
from conftest import run_ecctool_state_nodes, assert_nodes_size_is_equal

logger = logging.getLogger(__name__)


@pytest.mark.dependency(name="test_verify_install")
def test_verify_install(test_environment):
    assert test_environment.verify_node_count(4)
    out, err = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 4)


@pytest.mark.dependency(name="test_add_node", depends=["test_verify_install"])
def test_add_node(test_environment):
    test_environment.add_node()
    sleep(60)
    assert test_environment.verify_node_count(5)
    out, err = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 5)


@pytest.mark.dependency(name="test_remove_node", depends=["test_add_node"])
def test_remove_node(test_environment):
    test_environment.stop_extra_node()
    sleep(60)
    assert test_environment.verify_node_count(4)
    out, err = run_ecctool_state_nodes()
    assert_nodes_size_is_equal(out, 4)
