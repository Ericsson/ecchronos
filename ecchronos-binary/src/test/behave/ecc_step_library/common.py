#
# Copyright 2020 Telefonaktiebolaget LM Ericsson
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

from subprocess import Popen, PIPE
import re
import os
import json
import io
import requests
from jsonschema import validate
from behave import given, then, when  # pylint: disable=no-name-in-module


ID_PATTERN = r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
REPAIR_SUMMARY_PATTERN = r'Summary: \d+ completed, \d+ in queue, \d+ blocked, \d+ warning, \d+ error'
REPAIR_HEADER = r'| NodeID | JobID | Keyspace | Table | Status | Repaired(%) | Completed at | Repair type |'
REPAIR_ROW_FORMAT_PATTERN = r'\| .* \| .* \| {0} \| {1} \| (COMPLETED|IN_QUEUE|WARNING|ERROR) \| \d+[.]\d+ \| .* \| {2} \|'  # pylint: disable=line-too-long


def table_row(template, keyspace, table, repair_type=None):
    if repair_type:
        return template.format(keyspace, table, repair_type)
    return template.format(keyspace, table)


def strip_and_collapse(line):
    return re.sub(' +', ' ', line.rstrip().lstrip())


def match_and_remove_row(rows, expected_row):
    row_idx = -1
    found_row = None

    for idx, row in enumerate(rows):
        row = strip_and_collapse(row)
        if re.match(expected_row, row):
            found_row = row
            row_idx = int(idx)
            break

    assert row_idx != -1, "{0} not found in {1}".format(expected_row, rows)
    del rows[row_idx]
    return found_row


def handle_repair_output(context):
    output_data = context.out.decode('ascii').lstrip().rstrip().split('\n')
    context.header = output_data[0:3]
    context.rows = output_data[3:-1]
    context.summary = output_data[-1:]


def validate_header(header, expected_main_header):
    assert len(header) == 3, header

    assert header[0] == len(header[0]) * header[0][0], header[0]  # -----

    header[1] = strip_and_collapse(header[1])
    assert header[1] == expected_main_header, header[1]

    assert header[2] == len(header[2]) * header[2][0], header[2]  # -----


def validate_last_table_row(rows):
    assert len(rows) == 1, "Expecting last element to be '---' in {0}".format(rows)
    assert rows[0] == len(rows[0]) * rows[0][0], rows[0]  # -----
    assert len(rows) == 1, "{0} not empty".format(rows)


def get_job_id(context):
    out = context.out.decode('ascii')
    job_id = re.search(ID_PATTERN, out).group(0)
    assert job_id, "Could not find job id matching {0} in {1}".format(ID_PATTERN, out)
    return job_id


@given('we have access to ecctool')
def step_init(context):
    assert context.config.userdata.get("ecctool") is not False
    assert os.path.isfile(context.config.userdata.get("ecctool"))


@then('the output should contain a valid repair summary')
def step_validate_list_repairs_contains_summary(context):
    assert len(context.summary) == 1, "Expecting only 1 row summary"

    summary = context.summary[0]
    assert re.match(REPAIR_SUMMARY_PATTERN, summary), "Faulty summary '{0}'".format(summary)


@then('the output should not contain more rows')
def step_validate_list_rows_clear(context):
    validate_last_table_row(context.rows)


@then('the output should contain a valid repair header')
def step_validate_list_tables_header(context):
    validate_header(context.header, REPAIR_HEADER)


@then('the output should contain a repair row for {keyspace}.{table} with type {repair_type}')
def step_validate_repair_row(context, keyspace, table, repair_type):
    expected_row = table_row(REPAIR_ROW_FORMAT_PATTERN, keyspace, table, repair_type)
    match_and_remove_row(context.rows, expected_row)


def get_behave_dir():
    current_dir = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(current_dir, "../features"))


@given('I have a json schema {schema_name}')
def step_import_schema(context, schema_name):
    schema_file = os.path.join(get_behave_dir(), "schemas", "{0}.json".format(schema_name))

    with io.open(schema_file, "r", encoding="utf-8") as jsonfile:
        setattr(context, schema_name, json.loads(jsonfile.read()))


@given('I use the url {url}')
def step_set_url(context, url):
    context.url = url


@when('I send a GET request')
def step_send_get_request(context):
    assert context.url is not None
    assert not context.url.startswith("http"), "context.url cannot contain protocol 'http' or 'https'"
    client_cert = context.config.userdata.get("ecc_client_cert")
    client_key = context.config.userdata.get("ecc_client_key")
    client_ca = context.config.userdata.get("ecc_client_ca")
    if client_cert and client_key and client_ca:
        url = "https://" + context.url
        context.response = requests.get(url, cert=(client_cert, client_key), verify=client_ca, timeout=10)
    else:
        url = "http://" + context.url
        context.response = requests.get(url, timeout=10)


@when('I send a POST request')
def step_send_post_request(context):
    assert context.url is not None
    assert not context.url.startswith("http"), "context.url cannot contain protocol 'http' or 'https'"
    client_cert = context.config.userdata.get("ecc_client_cert")
    client_key = context.config.userdata.get("ecc_client_key")
    client_ca = context.config.userdata.get("ecc_client_ca")
    if client_cert and client_key and client_ca:
        url = "https://" + context.url
        context.response = requests.post(url, cert=(client_cert, client_key), verify=client_ca, timeout=10)
    else:
        url = "http://" + context.url
        context.response = requests.post(url, timeout=10)


@then('the response is successful')
def step_verify_response_is_successful(context):
    print( context.response.status_code )
    assert context.response is not None
    assert context.response.status_code == 200


@then('the response matches the json schema {schema_name}')
def step_verify_schema(context, schema_name):
    schema = getattr(context, schema_name, None)
    assert schema is not None

    context.json = context.response.json()

    validate(instance=context.json, schema=schema)


@then('the nodeid from response is extracted for {keyspace}.{table}')
def step_extract_id(context, keyspace, table):
    assert context.response is not None
    context.json = context.response.json()
    context.keyspace = keyspace
    context.table = table
    for obj in context.json:
        if obj["keyspace"] == keyspace and obj["table"] == table:
            context.id = obj["nodeID"]
            break
    assert context.id is not None

@then('the jobid from response is extracted for {keyspace}.{table}')
def step_extract_id(context, keyspace, table):
    assert context.response is not None
    context.json = context.response.json()
    for obj in context.json:
        if obj["keyspace"] == keyspace and obj["table"] == table:
            context.jobid = obj["jobID"]
            break
    assert context.jobid is not None


@then('the job list contains only keyspace {keyspace}')
def step_verify_job_list(context, keyspace):
    for obj in context.json:
        assert obj["keyspace"] == keyspace


def run_ecctool(context, params):
    cmd = [context.config.userdata.get("ecctool")] + params
    client_cert = context.config.userdata.get("ecc_client_cert")
    client_key = context.config.userdata.get("ecc_client_key")
    client_ca = context.config.userdata.get("ecc_client_ca")
    env = {}
    if client_cert and client_key and client_ca:
        env = {"ECCTOOL_CERT_FILE": client_cert, "ECCTOOL_KEY_FILE": client_key, "ECCTOOL_CA_FILE": client_ca}
    context.proc = Popen(cmd, stdout=PIPE, stderr=PIPE, env=env)  # pylint: disable=consider-using-with
    (context.out, context.err) = context.proc.communicate()
