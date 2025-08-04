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
import subprocess
from cass_config import CassandraCluster
from ecc_config import EcchronosConfig
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_CHECK = 10


def cassandra_cluster():
    context = CassandraCluster(global_vars.LOCAL)
    context.create_cluster()
    return context


def default_config(cassandra_cluster):
    ecc_config = EcchronosConfig(context=cassandra_cluster)
    ecc_config.modify_configuration()


def start_ecchronos():
    command = [f"{global_vars.BASE_DIR}/bin/ecctool", "start", "-p", global_vars.PIDFILE]

    result = subprocess.run(command, capture_output=True, text=True)

    logger.info("stdout:", result.stdout)
    logger.info("stderr:", result.stderr)
    logger.info("exit code:", result.returncode)


def check_status():
    checks = 0
    if global_vars.LOCAL:
        curl_cmd = ["curl", "--silent", "--fail", "--head", "--output", "/dev/null", global_vars.BASE_URL]
    else:
        curl_cmd = [
            "curl",
            "--silent",
            "--fail",
            "--head",
            "--output",
            "/dev/null",
            "--cert",
            f"{global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt",
            "--key",
            f"{global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem",
            "--cacert",
            f"{global_vars.CERTIFICATE_DIRECTORY}/serverca.crt",
            global_vars.BASE_URL,
        ]

    while checks <= MAX_CHECK:
        result = subprocess.run(curl_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            break

        log_path = os.path.join(global_vars.BASE_DIR, "ecc.debug.log")
        if os.path.isfile(log_path):
            logger.info("===Debug log content===")
            with open(log_path, "r") as f:
                print(f.read())
            logger.info("===Debug log content===")
        else:
            logger.info("No logs found")

        logger.info("...")
        checks += 1
        time.sleep(2)


def stop_ecchronos():
    command = [f"{global_vars.BASE_DIR}/bin/ecctool", "stop", "-p", global_vars.PIDFILE]

    result = subprocess.run(command, capture_output=True, text=True)

    logger.info("stdout:", result.stdout)
    logger.info("stderr:", result.stderr)
    logger.info("exit code:", result.returncode)


def run():
    context = CassandraCluster(global_vars.LOCAL)
    try:
        logger.info("Creating cluster")
        context.create_cluster()
        logger.info("Changing configs")
        default_config(context)
        logger.info("starting ecchronos")
        start_ecchronos()
        logger.info("checking status")
        check_status()
        logger.info("Sleeping")
        from time import sleep

        sleep(60)
        context.stop_cluster()
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        context.stop_cluster()


def stop():
    context = CassandraCluster(global_vars.LOCAL)
    context.stop_cluster()


run()
