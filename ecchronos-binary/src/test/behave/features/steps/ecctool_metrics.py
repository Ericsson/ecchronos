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

from behave import when, then  # pylint: disable=no-name-in-module
from ecc_step_library.common import run_ecctool


def run_ecc_metrics(context, params):
    run_ecctool(context, ["metrics"] + params)


@when("we fetch metrics")
def step_fetch_metrics(context):
    run_ecc_metrics(context, [])


@when('we fetch metrics filtered by name "{name}"')
def step_fetch_metrics_filtered(context, name):
    run_ecc_metrics(context, ["--name", name])


@when("we fetch metrics with raw output")
def step_fetch_metrics_raw(context):
    run_ecc_metrics(context, ["--raw"])


@then("the output should contain metrics exposition text")
def step_validate_metrics_exposition(context):
    assert context.proc.returncode == 0, context.err.decode("ascii")
    output = context.out.decode("ascii")
    assert "# HELP" in output or "# TYPE" in output, output


@then('the output should only contain metrics matching "{name}"')
def step_validate_metrics_name_filter(context, name):
    assert context.proc.returncode == 0, context.err.decode("ascii")
    normalized = name.lower().replace(".", "_")
    for line in context.out.decode("ascii").splitlines():
        stripped = line.strip()
        if not stripped or stripped == "# EOF":
            continue
        if stripped.startswith("#"):
            parts = stripped.split()
            if len(parts) >= 3 and parts[1] in ("HELP", "TYPE"):
                assert normalized in parts[2].lower().replace(".", "_"), line
        else:
            assert normalized in stripped.lower().replace(".", "_"), line


@then("the output should not contain metrics comments")
def step_validate_metrics_raw(context):
    assert context.proc.returncode == 0, context.err.decode("ascii")
    for line in context.out.decode("ascii").splitlines():
        stripped = line.strip()
        if stripped:
            assert not stripped.startswith("# HELP"), line
            assert not stripped.startswith("# TYPE"), line
