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

from behave import given  # pylint: disable=no-name-in-module


@given("I fetch schedules with nodeid and full")
def step_fetch_schedule_with_id_and_full(context):
    assert context.nodeid is not None
    context.url = "localhost:8080/repair-management/schedules/{0}?full=true".format(context.nodeid)


@given("I fetch schedules with nodeid")
def step_fetch_schedule_with_id(context):
    assert context.nodeid is not None
    context.url = "localhost:8080/repair-management/schedules/{0}".format(context.nodeid)

@given("I fetch schedules with nodeid and jobid")
def step_fetch_schedule_with_id(context):
    assert context.nodeid is not None
    assert context.jobid is not None
    context.url = "localhost:8080/repair-management/schedules/{0}/{1}".format(context.nodeid, context.jobid)

@given("I fetch schedules with nodeid and jobid and full")
def step_fetch_schedule_with_id(context):
    assert context.nodeid is not None
    assert context.jobid is not None
    context.url = "localhost:8080/repair-management/schedules/{0}/{1}?full=true".format(context.nodeid, context.jobid)

@given("I fetch schedules with nodeid keyspace {keyspace} and table {table}")
def step_fetch_schedule_with_id(context, keyspace, table):
    assert context.nodeid is not None
    assert keyspace is not None
    assert table is not None
    context.url = "localhost:8080/repair-management/schedules/{0}?keyspace={1}&table={2}".format(context.nodeid, keyspace, table)

@given("I fetch schedules with nodeid and keyspace {keyspace}")
def step_fetch_schedule_with_id(context, keyspace):
    assert context.nodeid is not None
    assert keyspace is not None
    context.url = "localhost:8080/repair-management/schedules/{0}?keyspace={1}".format(context.nodeid, keyspace)
