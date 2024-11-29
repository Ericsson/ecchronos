#
# Copyright 2022 Telefonaktiebolaget LM Ericsson
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

from behave import when  # pylint: disable=no-name-in-module
from ecc_step_library.common import handle_repair_output, run_ecctool


def run_ecc_run_repair(context, params):
    run_ecctool(context, ["run-repair"] + params)


@when("we run repair for keyspace {keyspace} and table {table}")
def step_run_repair(context, keyspace, table):
    run_ecc_run_repair(context, ["--keyspace", keyspace, "--table", table])
    handle_repair_output(context)


@when("we run repair for keyspace {keyspace}")
def step_run_repair_keyspace(context, keyspace):
    run_ecc_run_repair(context, ["--keyspace", keyspace])
    handle_repair_output(context)


@when("we run repair")
def step_run_repair_cluster(context):
    run_ecc_run_repair(context, [])
    handle_repair_output(context)


@when("we run local repair for keyspace {keyspace} and table {table} with type {repair_type}")
def step_run_local_repair(context, keyspace, table, repair_type):
    run_ecc_run_repair(context, ["--keyspace", keyspace, "--table", table, "--local", "--repair_type", repair_type])
    handle_repair_output(context)


@when("we run local repair for keyspace {keyspace}")
def step_run_local_repair_for_keyspace(context, keyspace):
    run_ecc_run_repair(context, ["--keyspace", keyspace, "--local"])
    handle_repair_output(context)


@when("we run local repair")
def step_run_local_repair_cluster(context):
    run_ecc_run_repair(context, ["--local"])
    handle_repair_output(context)
