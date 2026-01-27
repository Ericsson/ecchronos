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
import os
import logging
import sys
from conftest import build_behave_command

logger = logging.getLogger(__name__)


@pytest.mark.dependency(name="test_behave_tests")
def test_behave_tests(test_environment):
    """Test that runs behave tests"""
    from conftest import client

    cassandra_cluster = test_environment

    behave_cmd = build_behave_command(cassandra_cluster)

    logger.info("Running behave tests inside container")

    try:
        container = client.containers.get("ecchronos-agent")
        exec_result = container.exec_run(behave_cmd, workdir=f"{global_vars.CONTAINER_BASE_DIR}/behave", stream=False)

        exit_code = exec_result.exit_code
        output = exec_result.output.decode("utf-8")

        # Print output
        sys.stdout.write(output)
        sys.stdout.flush()

        logger.info(f"Behave tests completed with exit code: {exit_code}")

        if exit_code != 0:
            logger.error("Behave tests failed")
            pytest.fail("Behave tests failed")
        else:
            logger.info("Behave tests passed successfully")

    except Exception as e:
        logger.error(f"Failed to run behave tests: {e}")
        pytest.fail(f"Failed to run behave tests: {e}")


@pytest.mark.dependency(name="test_pylint_tests", depends=["test_behave_tests"])
def test_pylint_tests():
    """Test that runs pylint on specified directories"""
    directories = [
        f"{global_vars.PROJECT_BUILD_DIRECTORY}/../src/bin",
        f"{global_vars.PROJECT_BUILD_DIRECTORY}/../src/pylib/ecchronoslib",
        f"{global_vars.PROJECT_BUILD_DIRECTORY}/../src/test/behave/ecc_step_library",
        f"{global_vars.PROJECT_BUILD_DIRECTORY}/../src/test/behave/features/steps",
    ]

    for directory in directories:
        if not os.path.exists(directory):
            logger.warning(f"Directory {directory} does not exist, skipping pylint")
            continue

        logger.info(f"Running pylint for {directory}")
        try:
            result = subprocess.run(["pylint", directory], stdout=sys.stdout, stderr=sys.stderr, timeout=300)
            if result.returncode != 0:
                logger.error(f"Pylint failed for {directory}")
                pytest.fail(f"Pylint failed for {directory}")
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            logger.error(f"Failed to run pylint for {directory}: {e}")
            pytest.fail(f"Failed to run pylint for {directory}: {e}")

    logger.info("All pylint tests passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
