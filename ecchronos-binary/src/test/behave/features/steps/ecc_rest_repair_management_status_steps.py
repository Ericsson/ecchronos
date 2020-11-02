#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import json
import os
from jsonschema import validate
import requests
from behave import given, when, then  # pylint: disable=no-name-in-module


SCHEDULE_URL_FORMAT = "http://localhost:8080/repair-management/v1/schedule/keyspaces/{0}/tables/{1}"


def get_behave_dir():
    current_dir = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(current_dir, '..'))


@given(u'I have a json schema in {schema_name}.json')
def step_import_schema(context, schema_name):
    schema_file = os.path.join(get_behave_dir(), "{0}.json".format(schema_name))

    with open(schema_file, "r") as jsonfile:
        setattr(context, schema_name, json.loads(jsonfile.read()))


@given(u'we schedule an on demand repair on {keyspace}.{table}')
def schedule_demand_repair(context, keyspace, table):
    step_set_url(context, SCHEDULE_URL_FORMAT.format(keyspace, table))
    step_send_post_request(context)
    step_verify_response_is_succesful(context)


@given('I use the url {url}')
def step_set_url(context, url):
    context.url = url


@when('I send a GET request')
def step_send_get_request(context):
    assert context.url is not None
    context.response = requests.get(context.url)


@when('I send a POST request')
def step_send_post_request(context):
    assert context.url is not None
    context.response = requests.post(context.url)


@then('the response is successful')
def step_verify_response_is_succesful(context):
    assert context.response is not None
    assert context.response.status_code == 200


@then('the response matches the json {schema_name}')
def step_verify_schema(context, schema_name):
    schema = getattr(context, schema_name, None)
    assert schema is not None

    context.json = context.response.json()

    validate(instance=context.json, schema=schema)


@then('the job list contains only keyspace {keyspace}')
def step_verify_job_list(context, keyspace):
    for obj in context.json:
        assert obj["keyspace"] == keyspace
