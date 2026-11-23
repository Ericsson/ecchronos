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

"""The ecctool ``get-logging-level`` and ``set-logging-level`` subcommands (Issue #1999).

They read and change log levels of a running ecChronos agent, similar to
``nodetool getlogginglevel`` and ``nodetool setlogginglevel``.
"""

from __future__ import print_function

import sys
from argparse import ArgumentTypeError

from ecchronoslib import rest, table_printer
from ecchronoslib.common_args import ARG_OUTPUT_JSON, ARG_TIMEOUT, ARG_URL, add_common_arg

LEVELS = ("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "OFF")
RESET = "reset"

_PERSISTENCE_NOTE = (
    "The change is applied to the running agent only and is lost when it restarts. "
    "Very verbose levels such as TRACE can produce a lot of output and fill the disk on a busy agent."
)


def _level_argument(value):
    """Accept a level name in any case, or 'reset'."""
    normalized = value.strip()
    if normalized.lower() == RESET:
        return RESET
    if normalized.upper() in LEVELS:
        return normalized.upper()
    raise ArgumentTypeError("invalid level '{0}', must be one of {1} or {2}".format(value, ", ".join(LEVELS), RESET))


def _logger_argument(value):
    """Reject empty logger names and names the agent would not accept."""
    if not value.strip() or any(ch.isspace() or ord(ch) < 32 for ch in value):
        raise ArgumentTypeError("invalid logger name '{0}', it must not be empty or contain whitespace".format(value))
    return value


def add_logging_level_subcommands(sub_parsers):
    """Register get-logging-level and set-logging-level."""
    parser_get = sub_parsers.add_parser(
        "get-logging-level",
        description="Show the log levels of a running ecChronos agent. Without a logger, all loggers with an "
        "explicitly configured level (and ROOT) are shown. With a logger, both its configured and its effective "
        "level are shown; the configured level is empty when the logger inherits its level.",
    )
    parser_get.add_argument(
        "logger", nargs="?", type=_logger_argument, default=None, help="logger name, ROOT for the root logger"
    )
    add_common_arg(parser_get, ARG_OUTPUT_JSON)
    add_common_arg(parser_get, ARG_URL)
    add_common_arg(parser_get, ARG_TIMEOUT)

    parser_set = sub_parsers.add_parser(
        "set-logging-level",
        description="Set the log level of a logger in a running ecChronos agent without restarting it. "
        + _PERSISTENCE_NOTE,
    )
    parser_set.add_argument("logger", type=_logger_argument, help="logger name, ROOT for the root logger")
    parser_set.add_argument(
        "level",
        type=_level_argument,
        metavar="LEVEL",
        help="one of {0}, or '{1}' to clear the explicit level so the logger inherits again "
        "(not allowed for ROOT)".format(", ".join(LEVELS), RESET),
    )
    add_common_arg(parser_set, ARG_OUTPUT_JSON)
    add_common_arg(parser_set, ARG_URL)
    add_common_arg(parser_set, ARG_TIMEOUT)


def _fail(arguments, result):
    """Report a failed request and exit non-zero."""
    if arguments.output == "json":
        table_printer.output_json({"error": result.format_exception()})
    else:
        print(result.format_exception())
    sys.exit(1)


def _row(entry):
    return [entry.get("logger", ""), entry.get("configured_level") or "-", entry.get("effective_level") or "-"]


def _print_loggers(entries):
    """Print entries as an aligned table."""
    rows = [["LOGGER", "CONFIGURED", "EFFECTIVE"]] + [_row(entry) for entry in entries]
    widths = [max(len(row[col]) for row in rows) for col in range(3)]
    for row in rows:
        print("  ".join(cell.ljust(widths[col]) for col, cell in enumerate(row)).rstrip())


def get_logging_level(arguments):
    request = rest.LoggingRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.get(arguments.logger)
    if not result.is_successful():
        _fail(arguments, result)

    if arguments.output == "json":
        table_printer.output_json({"logging": result.data})
    elif arguments.logger is None:
        _print_loggers(result.data.get("loggers", []))
    else:
        _print_loggers([result.data])


def set_logging_level(arguments):
    request = rest.LoggingRequest(base_url=arguments.url, timeout=arguments.timeout)
    result = request.put(arguments.logger, arguments.level)
    if not result.is_successful():
        _fail(arguments, result)

    if arguments.output == "json":
        table_printer.output_json({"logging": result.data})
    else:
        _print_loggers([result.data])
