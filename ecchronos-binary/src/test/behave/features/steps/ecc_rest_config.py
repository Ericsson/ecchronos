#
# Copyright 2026 Telefonaktiebolaget LM Ericsson
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
import requests
from behave import when, then  # pylint: disable=no-name-in-module


@when("I send a PATCH request with body {body}")
def step_send_patch_request(context, body):
    assert context.url is not None
    client_cert = context.config.userdata.get("ecc_client_cert")
    client_key = context.config.userdata.get("ecc_client_key")
    client_ca = context.config.userdata.get("ecc_client_ca")
    headers = {"Content-Type": "application/json"}
    if client_cert and client_key and client_ca:
        url = "https://" + context.url
        context.response = requests.patch(url, data=body, headers=headers,
                                          cert=(client_cert, client_key), verify=client_ca, timeout=10)
    else:
        url = "http://" + context.url
        context.response = requests.patch(url, data=body, headers=headers, timeout=10)


@then("the response contains {field}")
def step_response_contains_field(context, field):
    assert context.response is not None
    data = context.response.json()
    assert field in data, f"Field '{field}' not found in response: {data}"


@then("the response has {field} equal to {value}")
def step_response_field_equals(context, field, value):
    assert context.response is not None
    data = context.response.json()
    assert field in data, f"Field '{field}' not found in response: {data}"
    expected = json.loads(value)
    actual = data[field]
    assert actual == expected, f"Expected {field}={expected}, got {actual}"
