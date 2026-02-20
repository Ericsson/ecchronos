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
        ROOT_DIR, BASE_URL, CASSANDRA_CERT_PATH, CASSANDRA_VERSION, BASE_URL_TLS, JOLOKIA_ENABLED, \
        PEM_ENABLED, CONTAINER_BASE_DIR , CONTAINER_CONF_PATH, CONTAINER_ECC_YAML_PATH, \
        CONTAINER_APPLICATION_YAML_PATH, CONTAINER_SECURITY_YAML_PATH, CONTAINER_SCHEDULE_YAML_PATH, \
        CONTAINER_LOGBACK_FILE_PATH, CONTAINER_JVM_OPTION_PATH, CONTAINER_CERTIFICATE_PATH, CONTAINER_LOGS_PATH, \
        HOST_LOGS_PATH, JAVA_VERSION, ECC_CONTAINER_IP, DEFAULT_INITIAL_CONTACT_POINT, DC1, DC2, \
        DEFAULT_AGENT_TYPE, DEFAULT_INSTANCE_NAME, DEFAULT_SEED_IP_DC2
    # fmt:on

    TEST_DIR = os.path.dirname(os.path.realpath(__file__))
    VENV_DIR = os.path.join(TEST_DIR, "venv")
    PIDFILE = os.path.join(TEST_DIR, "ecc.pid")
    PROJECT_VERSION = os.environ.get("PROJECT_VERSION")
    PROJECT_BUILD_DIRECTORY = os.environ.get("PROJECT_BUILD_DIRECTORY")
    BASE_DIR = f"{TEST_DIR}/ecchronos-binary-agent-{PROJECT_VERSION}"
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
    JAVA_VERSION = os.environ.get("JAVA_VERSION")
    JOLOKIA_ENABLED = os.environ.get("JOLOKIA_ENABLED")
    LOCAL = os.environ.get("LOCAL")
    PEM_ENABLED = os.environ.get("PEM_ENABLED")
    ECC_CONTAINER_IP = "172.29.0.7"
    BASE_URL = "http://{ip}:8080/repair-management/schedules"
    BASE_URL_TLS = "https://{ip}:8080/repair-management/schedules"

    CONTAINER_BASE_DIR = f"/opt/ecchronos/ecchronos-binary-agent-{PROJECT_VERSION}"
    CONTAINER_CONF_PATH = f"{CONTAINER_BASE_DIR}/conf"
    CONTAINER_ECC_YAML_PATH = f"{CONTAINER_CONF_PATH}/ecc.yml"
    CONTAINER_APPLICATION_YAML_PATH = f"{CONTAINER_CONF_PATH}/application.yml"
    CONTAINER_SECURITY_YAML_PATH = f"{CONTAINER_CONF_PATH}/security.yml"
    CONTAINER_SCHEDULE_YAML_PATH = f"{CONTAINER_CONF_PATH}/schedule.yml"
    CONTAINER_LOGBACK_FILE_PATH = f"{CONTAINER_CONF_PATH}/logback.xml"
    CONTAINER_JVM_OPTION_PATH = f"{CONTAINER_CONF_PATH}/jvm.options"
    CONTAINER_CERTIFICATE_PATH = "/etc/certificates"
    CONTAINER_LOGS_PATH = "/var/log/ecchronos"
    HOST_LOGS_PATH = f"{BASE_DIR}/logs"
    DEFAULT_INITIAL_CONTACT_POINT = "172.29.0.2"
    DEFAULT_SEED_IP_DC2 = "172.29.0.3"
    DC1 = "datacenter1"
    DC2 = "datacenter2"
    DEFAULT_AGENT_TYPE = "datacenterAware"
    DEFAULT_INSTANCE_NAME = "ecchronos-agent-cluster-wide"


get()
