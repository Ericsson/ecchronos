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
import global_variables as global_vars


class EcchronosConfig:
    def __init__(self, context):
        self.context = context

    def modify_configuration(self):
        self._uncomment_head_options()
        self._modify_connection_configuration()
        self._modify_scheduler_configuration()

        if self.context.local != "true":
            self._modify_security_configuration()
            self._modify_application_configuration()

        self._modify_logback_configuration()
        self._modify_schedule_configuration()

    def _uncomment_head_options(self):
        pattern = re.compile(r"^#\s*(-X.*)")
        with open(global_vars.JVM_OPTIONS_FILE_PATH, "r", encoding="utf-8") as file:
            lines = file.readlines()

        with open(global_vars.JVM_OPTIONS_FILE_PATH, "w", encoding="utf-8") as file:
            for line in lines:
                match = pattern.match(line)
                if match:
                    file.write(match.group(1) + "\n")
                else:
                    file.write(line)

    def _modify_connection_configuration(self):
        data = self._read_yaml_data(global_vars.ECC_YAML_FILE_PATH)
        data["connection"]["cql"]["host"] = self.context.cassandra_ip
        data["connection"]["jmx"]["host"] = self.context.cassandra_ip
        data["connection"]["cql"]["port"] = self.context.cassandra_native_port
        data["connection"]["jmx"]["port"] = self.context.cassandra_jmx_port
        self._modify_yaml_data(global_vars.ECC_YAML_FILE_PATH, data)

    def _modify_scheduler_configuration(self):
        data = self._read_yaml_data(global_vars.ECC_YAML_FILE_PATH)
        data["scheduler"]["frequency"]["time"] = 1
        self._modify_yaml_data(global_vars.ECC_YAML_FILE_PATH, data)

    def _modify_security_configuration(self):
        data = self._read_yaml_data(global_vars.SECURITY_YAML_FILE_PATH)
        data["cql"]["credentials"]["enabled"] = True
        data["cql"]["credentials"]["username"] = "eccuser"
        data["cql"]["credentials"]["password"] = "eccpassword"
        data["cql"]["tls"]["enabled"] = True
        data["cql"]["tls"]["keystore"] = f"{global_vars.CERTIFICATE_DIRECTORY}/.keystore"
        data["cql"]["tls"]["keystore_password"] = "ecctest"
        data["cql"]["tls"]["truststore"] = f"{global_vars.CERTIFICATE_DIRECTORY}/.truststore"
        data["cql"]["tls"]["truststore_password"] = "ecctest"
        self._modify_yaml_data(global_vars.SECURITY_YAML_FILE_PATH, data)

    def _modify_application_configuration(self):
        data = self._read_yaml_data(global_vars.APPLICATION_YAML_FILE_PATH)

        if "server" not in data:
            data["server"] = {}
        if "ssl" not in data["server"]:
            data["server"]["ssl"] = {}

        data["server"]["ssl"]["enabled"] = True
        data["server"]["ssl"]["key-store"] = f"{global_vars.CERTIFICATE_DIRECTORY}/serverkeystore"
        data["server"]["ssl"]["key-store-password"] = "ecctest"
        data["server"]["ssl"]["key-store-type"] = "PKCS12"
        data["server"]["ssl"]["key-alias"] = "1"
        data["server"]["ssl"]["trust-store"] = f"{global_vars.CERTIFICATE_DIRECTORY}/servertruststore"
        data["server"]["ssl"]["trust-store-password"] = "ecctest"
        data["server"]["ssl"]["client-auth"] = "need"
        data["springdoc"]["api-docs"]["enabled"] = True
        data["springdoc"]["api-docs"]["show-actuator"] = True
        self._modify_yaml_data(global_vars.APPLICATION_YAML_FILE_PATH, data)

    def _modify_logback_configuration(self):
        with open(global_vars.LOGBACK_FILE_PATH, "r") as file:
            lines = file.readlines()

        pattern = re.compile(r'^(\s*)(<appender-ref ref="STDOUT" />)\s*$')

        with open(global_vars.LOGBACK_FILE_PATH, "w") as file:
            for line in lines:
                match = pattern.match(line)
                if match:
                    indent = match.group(1)
                    content = match.group(2)
                    new_line = f"{indent}<!-- {content} -->\n"
                    file.write(new_line)
                else:
                    file.write(line)

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
                        "alarm": {"warn": {"time": 4, "unit": "days"}, "error": {"time": 8, "unit": "days"}},
                    }
                ],
            },
            {
                "name": "test2",
                "tables": [
                    {"name": "table1", "repair_type": "incremental"},
                    {"name": "table2", "repair_type": "parallel_vnode"},
                ],
            },
        ]
        self._modify_yaml_data(global_vars.SCHEDULE_YAML_FILE_PATH, data)

    def _read_yaml_data(self, filename):
        with open(filename, "r") as f:
            data = yaml.safe_load(f)
            return data

    def _modify_yaml_data(self, filename, data):
        with open(filename, "w") as file:
            yaml.dump(data, file, sort_keys=False)
