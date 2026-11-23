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

"""Command line arguments shared by the ecctool subcommands."""

ARG_OUTPUT_JSON = {
    "flags": ["-o", "--output"],
    "type": str,
    "help": "output formats: json (defaults to no format)",
    "default": "",
}
ARG_URL = {
    "flags": ["-u", "--url"],
    "type": str,
    "help": "ecchronos host URL (format: http://<host>:<port>)",
    "default": None,
}
ARG_TIMEOUT = {
    "flags": ["-T", "--timeout"],
    "type": float,
    "help": "request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)",
    "default": None,
}


def add_common_arg(parser, arg_config, required=None):
    """Helper function to add common arguments to parsers."""
    kwargs = {k: v for k, v in arg_config.items() if k != "flags"}
    if required is not None:
        kwargs["required"] = required
    parser.add_argument(*arg_config["flags"], **kwargs)
