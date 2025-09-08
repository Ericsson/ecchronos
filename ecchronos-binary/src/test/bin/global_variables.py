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

import os


def get():
    # pylint: disable=global-statement, global-variable-undefined, too-many-statements
    # fmt:off
    global TEST_DIR, VENV_DIR, PIDFILE, PROJECT_VERSION, PROJECT_BUILD_DIRECTORY, \
        BASE_DIR, CONF_DIR, PYLIB_DIR, CERTIFICATE_DIRECTORY, ECC_YAML_FILE_PATH, \
        SECURITY_YAML_FILE_PATH, APPLICATION_YAML_FILE_PATH, SCHEDULE_YAML_FILE_PATH, \
        LOGBACK_FILE_PATH, JVM_OPTIONS_FILE_PATH, LOCAL, CASSANDRA_DOCKER_COMPOSE_FILE_PATH, \
        ROOT_DIR, BASE_URL, CASSANDRA_CERT_PATH, CASSANDRA_VERSION, BASE_URL_TLS, JOLOKIA_ENABLED
    # fmt:on

    TEST_DIR = os.path.dirname(os.path.realpath(__file__))
    VENV_DIR = os.path.join(TEST_DIR, "venv")
    PIDFILE = os.path.join(TEST_DIR, "ecc.pid")
    PROJECT_VERSION = os.environ.get("PROJECT_VERSION")
    PROJECT_BUILD_DIRECTORY = os.environ.get("PROJECT_BUILD_DIRECTORY")
    BASE_DIR = f"{TEST_DIR}/ecchronos-binary-{PROJECT_VERSION}"
    CONF_DIR = f"{BASE_DIR}/conf"
    PYLIB_DIR = f"{BASE_DIR}/pylib"
    CERTIFICATE_DIRECTORY = f"{PROJECT_BUILD_DIRECTORY}/certificates/cert"
    ECC_YAML_FILE_PATH = f"{CONF_DIR}/ecc.yml"
    SECURITY_YAML_FILE_PATH = f"{CONF_DIR}/security.yml"
    APPLICATION_YAML_FILE_PATH = f"{CONF_DIR}/application.yml"
    SCHEDULE_YAML_FILE_PATH = f"{CONF_DIR}/schedule.yml"
    LOGBACK_FILE_PATH = f"{CONF_DIR}/logback.xml"
    JVM_OPTIONS_FILE_PATH = f"{CONF_DIR}/jvm.options"
    ROOT_DIR = os.path.dirname(os.environ.get("BASE_DIR"))
    CASSANDRA_DOCKER_COMPOSE_FILE_PATH = f"{ROOT_DIR}/cassandra-test-image/src/main/docker"
    CASSANDRA_CERT_PATH = f"{ROOT_DIR}/cassandra-test-image/target/certificates/cert"
    CASSANDRA_VERSION = os.environ.get("CASSANDRA_VERSION")
    JOLOKIA_ENABLED = os.environ.get("JOLOKIA_ENABLED")
    LOCAL = os.environ.get("LOCAL")
    BASE_URL = "http://localhost:8080/repair-management/schedules"
    BASE_URL_TLS = "https://localhost:8080/repair-management/schedules"


get()
