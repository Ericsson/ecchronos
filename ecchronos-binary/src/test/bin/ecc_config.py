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

import yaml
import re
import tempfile
import global_variables as global_vars

DC1 = "datacenter1"
DC2 = "datacenter2"
DEFAULT_AGENT_TYPE = "datacenterAware"


class EcchronosConfig:
    def __init__(
        self,
        datacenter_aware=None,
        local_dc=DC1,
        agent_type=DEFAULT_AGENT_TYPE,
        initial_contact_point=global_vars.DEFAULT_INITIAL_CONTACT_POINT,
    ):
        self.container_mounts = {}
        self.datacenter_aware = [{"name": DC1}, {"name": DC2}] if datacenter_aware is None else datacenter_aware
        self.local_dc = local_dc
        self.agent_type = agent_type
        self.initial_contact_point = initial_contact_point

    def modify_configuration(self):
        self._modify_ecc_yaml_file()
        self._modify_security_yaml_file()
        self._modify_application_yaml_file()
        self._modify_schedule_configuration()
        self._uncomment_head_options()
        self._modify_logback_configuration()
        self.container_mounts["certificates"] = {
            "host": global_vars.CERTIFICATE_DIRECTORY,
            "container": global_vars.CONTAINER_CERTIFICATE_PATH,
        }
        self.container_mounts["logs"] = {
            "host": global_vars.HOST_LOGS_PATH,
            "container": global_vars.CONTAINER_LOGS_PATH,
        }

    # Modify ecc.yaml file
    def _modify_ecc_yaml_file(self):
        data = self._read_yaml_data(global_vars.ECC_YAML_FILE_PATH)
        data = self._modify_connection_configuration(data)
        data = self._modify_scheduler_configuration(data)
        data = self._modify_twcs_configuration(data)
        if global_vars.JOLOKIA_ENABLED == "true":
            data = self._modify_jolokia_configuration(data)
        self.container_mounts["ecc"] = {"host": self.write_tmp(data), "container": global_vars.CONTAINER_ECC_YAML_PATH}

    def _modify_connection_configuration(self, data):
        data["connection"]["cql"]["contactPoints"] = [{"host": self.initial_contact_point, "port": 9042}]
        data["connection"]["cql"]["datacenterAware"]["datacenters"] = self.datacenter_aware
        return data

    def _modify_scheduler_configuration(self, data):
        data["scheduler"]["frequency"]["time"] = 1
        return data

    def _modify_twcs_configuration(self, data):
        data["repair"]["ignore_twcs_tables"] = True
        return data

    def _modify_jolokia_configuration(self, data):
        data["connection"]["jmx"]["jolokia"]["enabled"] = True
        if global_vars.PEM_ENABLED == "true":
            data["connection"]["jmx"]["jolokia"]["port"] = 8443
            data["connection"]["jmx"]["jolokia"]["usePem"] = True
        return data

    # Modify security.yaml file
    def _modify_security_yaml_file(self):
        data = self._read_yaml_data(global_vars.SECURITY_YAML_FILE_PATH)
        if global_vars.LOCAL != "true":
            data = self._modify_security_configuration(data)
        else:
            data = self._modify_cql_configuration(data)
        self.container_mounts["security"] = {
            "host": self.write_tmp(data),
            "container": global_vars.CONTAINER_SECURITY_YAML_PATH,
        }

    def _modify_security_configuration(self, data):
        data["cql"]["credentials"]["enabled"] = True
        data["cql"]["credentials"]["username"] = "eccuser"
        data["cql"]["credentials"]["password"] = "eccpassword"
        data["cql"]["tls"]["enabled"] = True
        data["cql"]["tls"]["keystore"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/.keystore"
        data["cql"]["tls"]["keystore_password"] = "ecctest"
        data["cql"]["tls"]["truststore"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/.truststore"
        data["cql"]["tls"]["truststore_password"] = "ecctest"

        data["jmx"]["credentials"]["enabled"] = True
        data["jmx"]["credentials"]["username"] = "cassandra"
        data["jmx"]["credentials"]["password"] = "cassandra"
        data["jmx"]["tls"]["enabled"] = True
        if global_vars.PEM_ENABLED != "true":
            data["jmx"]["tls"]["keystore"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/.keystore"
            data["jmx"]["tls"]["keystore_password"] = "ecctest"
            data["jmx"]["tls"]["truststore"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/.truststore"
            data["jmx"]["tls"]["truststore_password"] = "ecctest"
        else:
            data["jmx"]["tls"]["certificate"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/pem/clientcert.crt"
            data["jmx"]["tls"][
                "certificate_private_key"
            ] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/pem/clientkey.pem"
            data["jmx"]["tls"]["trust_certificate"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/pem/serverca.crt"
        return data

    def _modify_cql_configuration(self, data):
        data["cql"]["credentials"]["enabled"] = True
        data["cql"]["credentials"]["username"] = "cassandra"
        data["cql"]["credentials"]["password"] = "cassandra"
        return data

    # Modify application.yaml file
    def _modify_application_yaml_file(self):
        data = self._read_yaml_data(global_vars.APPLICATION_YAML_FILE_PATH)
        if global_vars.LOCAL != "true":
            data = self._modify_application_configuration(data)
        data = self._modify_spring_doc_configuration(data)
        self.container_mounts["application"] = {
            "host": self.write_tmp(data),
            "container": global_vars.CONTAINER_APPLICATION_YAML_PATH,
        }

    def _modify_application_configuration(self, data):
        if "server" not in data:
            data["server"] = {}
        if "ssl" not in data["server"]:
            data["server"]["ssl"] = {}

        data["server"]["ssl"]["enabled"] = True
        data["server"]["ssl"]["key-store"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/serverkeystore"
        data["server"]["ssl"]["key-store-password"] = "ecctest"
        data["server"]["ssl"]["key-store-type"] = "PKCS12"
        data["server"]["ssl"]["key-alias"] = "1"
        data["server"]["ssl"]["trust-store"] = f"{global_vars.CONTAINER_CERTIFICATE_PATH}/servertruststore"
        data["server"]["ssl"]["trust-store-password"] = "ecctest"
        data["server"]["ssl"]["client-auth"] = "need"
        return data

    def _modify_spring_doc_configuration(self, data):
        data["springdoc"]["api-docs"]["enabled"] = True
        data["springdoc"]["api-docs"]["show-actuator"] = True
        return data

    def _uncomment_head_options(self):
        pattern = re.compile(r"^#\s*(-X.*)")
        with open(global_vars.JVM_OPTIONS_FILE_PATH, "r", encoding="utf-8") as file:
            lines = file.readlines()

        result = []

        for line in lines:
            match = pattern.match(line)
            if match:
                result.append(match.group(1) + "\n")
            else:
                result.append(line)

        self.container_mounts["jvm"] = {
            "host": self.write_tmp(result, ".options"),
            "container": global_vars.CONTAINER_JVM_OPTION_PATH,
        }

    def _modify_logback_configuration(self):
        with open(global_vars.LOGBACK_FILE_PATH, "r") as file:
            lines = file.readlines()

        pattern = re.compile(r'^(\s*)(<appender-ref ref="STDOUT" />)\s*$')

        result = []

        for line in lines:
            match = pattern.match(line)
            if match:
                indent = match.group(1)
                content = match.group(2)
                result.append(f"{indent}<!-- {content} -->\n")
            else:
                result.append(line)

        self.container_mounts["logback"] = {
            "host": self.write_tmp(result, ".xml"),
            "container": global_vars.CONTAINER_LOGBACK_FILE_PATH,
        }

    def _modify_schedule_configuration(self):
        data = self._read_yaml_data(global_vars.SCHEDULE_YAML_FILE_PATH)
        data["keyspaces"] = [
            {
                "name": "test",
                "tables": [
                    {
                        "name": "table1",
                        "interval": {"time": 1, "unit": "days"},
                        "initial_delay": {"time": 1, "unit": "hours"},
                        "unwind_ratio": 0.1,
                    },
                    {"name": "table3", "enabled": False},
                ],
            },
            {
                "name": "test2",
                "tables": [
                    {"name": "table1", "repair_type": "incremental"},
                    {"name": "table2", "repair_type": "parallel_vnode"},
                ],
            },
            {
                "name": "system_auth",
                "tables": [
                    {"name": "network_permissions", "enabled": False},
                    {"name": "resource_role_permissons_index", "enabled": False},
                    {"name": "role_members", "enabled": False},
                    {"name": "role_permissions", "enabled": False},
                    {"name": "roles", "enabled": False},
                ],
            },
            {
                "name": "ecchronos",
                "tables": [
                    {"name": "lock", "enabled": False},
                    {"name": "lock_priority", "enabled": False},
                    {"name": "on_demand_repair_status", "enabled": False},
                    {"name": "reject_configuration", "enabled": False},
                    {"name": "repair_history", "enabled": True},
                ],
            },
        ]
        self.container_mounts["schedule"] = {
            "host": self.write_tmp(data),
            "container": global_vars.CONTAINER_SCHEDULE_YAML_PATH,
        }

    def _read_yaml_data(self, filename):
        with open(filename, "r") as f:
            data = yaml.safe_load(f)
            return data

    def write_tmp(self, data, suffix=".yaml") -> str:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
        if suffix == ".yaml":
            yaml.safe_dump(data, tmp)
        else:
            tmp.writelines(data)
        tmp.close()
        return tmp.name
