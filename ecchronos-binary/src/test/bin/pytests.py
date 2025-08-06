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
import sys
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

    logger.info(f"stdout: {result.stdout}")
    logger.info(f"stderr: {result.stderr}")
    logger.info(f"exit code: {result.returncode}")


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


def run_behave(context):
    command = []
    if global_vars.LOCAL == "true" :
        command = [
            "behave",
            "--define",
            f"ecctool={global_vars.BASE_DIR}/bin/ecctool",
            "--define",
            f"cassandra_address={context.cassandra_ip}",
            "--define",
            "no_tls",
        ]
    else:
        command = [
            "behave",
            "--define",
            f"ecctool={global_vars.BASE_DIR}/bin/ecctool",
            "--define",
            f"cassandra_address={context.cassandra_ip}",
            "--define",
            f"ecc_client_cert={global_vars.CERTIFICATE_DIRECTORY}/clientcert.crt",
            "--define",
            f"ecc_client_key={global_vars.CERTIFICATE_DIRECTORY}/clientkey.pem",
            "--define",
            f"ecc_client_ca={global_vars.CERTIFICATE_DIRECTORY}/serverca.crt",
            "--define",
            "cql_user=eccuser",
            "--define",
            "cql_password=eccpassword",
            "--define",
            f"cql_client_cert={global_vars.CERTIFICATE_DIRECTORY}/cert.crt",
            "--define",
            f"cql_client_key={global_vars.CERTIFICATE_DIRECTORY}/key.pem",
            "--define",
            f"cql_client_ca={global_vars.CERTIFICATE_DIRECTORY}/ca.crt",
        ]
    subprocess.run(command, stdout=sys.stdout, stderr=sys.stderr)

def run():
    context = CassandraCluster(global_vars.LOCAL)
    try:
        logger.info(f"Creating cluster with version {global_vars.CASSANDRA_VERSION}")
        context.create_cluster()
        logger.info("Changing configs")
        default_config(context)
        logger.info("starting ecchronos")
        start_ecchronos()
        logger.info("checking status")
        check_status()
        logger.info("Sleeping")
        run_behave(context)
        stop_ecchronos()
        context.stop_cluster()
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        context.stop_cluster()
        sys.exit(1)


def stop():
    context = CassandraCluster(global_vars.LOCAL)
    context.stop_cluster()


if __name__ == "__main__":
    run()
