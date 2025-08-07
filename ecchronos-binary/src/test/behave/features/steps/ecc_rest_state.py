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

from behave import given, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import (
    get_job_id,
    match_and_remove_row,
    strip_and_collapse,
    validate_header,
    step_validate_list_rows_clear,
    run_ecctool,
    table_row,
)  # pylint: disable=line-too-long


@then("the number of nodes is {count}")
def step_check_node_count(context, count):
    assert context.response is not None
    context.json = context.response.json()
    print("Number of JSON objects:", len(context.json) )

#    for obj in context.json:
#        if obj["keyspace"] == keyspace and obj["table"] == table:
#            context.id = obj["nodeID"]
#            break
    assert len(context.json) == int(count)
