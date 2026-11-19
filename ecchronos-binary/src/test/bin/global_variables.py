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


def _read_env_file_value(env_file, key):
    """Return the value of key from a docker compose style .env file, or None."""
    if not os.path.isfile(env_file):
        return None
    value = None
    with open(env_file, encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, raw = line.partition("=")
            if name.strip() == key:
                value = raw.strip().strip("\"'")
    return value


def _test_net_prefix(compose_dir):
    """Test network prefix, from the environment or the compose .env file (same precedence as compose)."""
    env_file = os.path.join(compose_dir, ".env")
    prefix = os.environ.get("ECC_TEST_NET_PREFIX") or _read_env_file_value(env_file, "ECC_TEST_NET_PREFIX")
    if not prefix:
        raise RuntimeError(
            f"ECC_TEST_NET_PREFIX is not set in the environment or in {env_file}. "
            "It defines the integration test subnet."
        )
    return prefix


def get():
    # pylint: disable=global-statement, global-variable-undefined, too-many-statements
    # fmt:off
    global TEST_DIR, VENV_DIR, PIDFILE, PROJECT_VERSION, PROJECT_BUILD_DIRECTORY, \
        BASE_DIR, CONF_DIR, PYLIB_DIR, CERTIFICATE_DIRECTORY, ECC_YAML_FILE_PATH, \
        SECURITY_YAML_FILE_PATH, APPLICATION_YAML_FILE_PATH, SCHEDULE_YAML_FILE_PATH, \
        LOGBACK_FILE_PATH, JVM_OPTIONS_FILE_PATH, LOCAL, CASSANDRA_DOCKER_COMPOSE_FILE_PATH, \
        ROOT_DIR, BASE_URL, CASSANDRA_CERT_PATH, CASSANDRA_VERSION, BASE_URL_TLS, JOLOKIA_ENABLED, \
        JOLOKIA_VERSION, \
        PEM_ENABLED, CONTAINER_BASE_DIR , CONTAINER_CONF_PATH, CONTAINER_ECC_YAML_PATH, \
        CONTAINER_APPLICATION_YAML_PATH, CONTAINER_SECURITY_YAML_PATH, CONTAINER_SCHEDULE_YAML_PATH, \
        CONTAINER_LOGBACK_FILE_PATH, CONTAINER_JVM_OPTION_PATH, CONTAINER_CERTIFICATE_PATH, CONTAINER_LOGS_PATH, \
        HOST_LOGS_PATH, JAVA_VERSION, ECC_CONTAINER_IP, DEFAULT_INITIAL_CONTACT_POINT, DC1, DC2, \
        DEFAULT_AGENT_TYPE, DEFAULT_INSTANCE_NAME, DEFAULT_SEED_IP_DC2, ECC_TEST_NET_PREFIX, \
        EXTRA_NODE_IP, SECOND_AGENT_IP
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
    # Must match the pom's jolokia.adapter.version, forwarded via the JOLOKIA_VERSION env var.
    # No default: a missing value means the pom/env wiring is broken, so fail fast rather than
    # silently building the Cassandra image with a mismatched Jolokia agent.
    JOLOKIA_VERSION = os.environ.get("JOLOKIA_VERSION")
    if not JOLOKIA_VERSION:
        raise RuntimeError(
            "JOLOKIA_VERSION environment variable is not set. It must be injected from the pom "
            "property jolokia.adapter.version."
        )
    LOCAL = os.environ.get("LOCAL")
    PEM_ENABLED = os.environ.get("PEM_ENABLED")
    # All test cluster addresses derive from the prefix in cassandra-test-image/src/main/docker/.env.
    ECC_TEST_NET_PREFIX = _test_net_prefix(CASSANDRA_DOCKER_COMPOSE_FILE_PATH)
    ECC_CONTAINER_IP = f"{ECC_TEST_NET_PREFIX}.7"
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
    DEFAULT_INITIAL_CONTACT_POINT = f"{ECC_TEST_NET_PREFIX}.2"
    DEFAULT_SEED_IP_DC2 = f"{ECC_TEST_NET_PREFIX}.3"
    # Extra Cassandra node added by the topology tests, and the second (dc2) agent.
    EXTRA_NODE_IP = f"{ECC_TEST_NET_PREFIX}.6"
    SECOND_AGENT_IP = f"{ECC_TEST_NET_PREFIX}.8"
    DC1 = "datacenter1"
    DC2 = "datacenter2"
    DEFAULT_AGENT_TYPE = "datacenterAware"
    DEFAULT_INSTANCE_NAME = "ecchronos-agent-cluster-wide"


get()
