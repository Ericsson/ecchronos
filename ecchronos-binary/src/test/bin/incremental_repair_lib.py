#!/usr/bin/env python3
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

import logging

from conftest import get_schedules_json

logger = logging.getLogger(__name__)

REPAIR_TYPE_VNODE = "VNODE"
REPAIR_TYPE_INCREMENTAL = "INCREMENTAL"

# lastRepaired sentinel from ecchronoslib meaning "never repaired yet".
NEVER_REPAIRED = -1

# Statuses that mean the table is keeping up / finished a cycle.
HEALTHY_STATUSES = ("COMPLETED", "ON_TIME")


def repair_schedule_keyspaces(keyspace, table, repair_type, interval_minutes):
    """Build a schedule.yml 'keyspaces' list containing exactly one scheduled table.

    The target table is declared with the given repair_type and a short interval / zero
    initial delay so a cycle fires promptly within the test window. The ecchronos and
    system_auth keyspaces are disabled so the target table is the ONLY schedule ecChronos
    produces - which keeps "assert everything reports <type>" unambiguous.
    """
    return [
        {
            "name": keyspace,
            "tables": [
                {
                    "name": table,
                    "repair_type": repair_type.lower(),
                    "interval": {"time": interval_minutes, "unit": "minutes"},
                    "initial_delay": {"time": 0, "unit": "minutes"},
                    "unwind_ratio": 0.0,
                }
            ],
        },
        {"name": "system_auth", "tables": [{"name": ".*", "enabled": False}]},
        {"name": "ecchronos", "tables": [{"name": ".*", "enabled": False}]},
    ]


def create_schema(cluster, keyspace, table):
    """Create the test keyspace (datacenter1:2, datacenter2:1 -> present on all 4 nodes)."""
    cluster.run_cql(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1};"
    )
    cluster.run_cql(f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} (key text PRIMARY KEY, value text);")


def write_data(cluster, keyspace, table, rows=1000, offset=0):
    """Insert rows so repair has data to reconcile; offset namespaces each batch's keys."""
    statements = [
        f"INSERT INTO {keyspace}.{table} (key, value) VALUES ('k{offset + i}', 'v{offset + i}');" for i in range(rows)
    ]
    cluster.run_cql("\n".join(statements))
    logger.info("Inserted %s rows into %s.%s (offset %s)", rows, keyspace, table, offset)


def schedules_for(keyspace, table):
    """Schedules for keyspace.table from 'ecctool schedules -o json' (one dict per node)."""
    return get_schedules_json(["--keyspace", keyspace, "--table", table])


def max_last_repaired(schedules):
    """Highest real lastRepaired across per-node schedules (NEVER_REPAIRED if none)."""
    values = [s["last_repaired_at_in_ms"] for s in schedules if s["last_repaired_at_in_ms"] != NEVER_REPAIRED]
    return max(values) if values else NEVER_REPAIRED


def max_repaired_ratio(schedules):
    """Highest repaired_ratio across per-node schedules (0.0 if none)."""
    return max((s["repaired_ratio"] for s in schedules), default=0.0)


def assert_all_repair_type(rows, expected_type, context, keyspace, table):
    """Assert every row (schedule or repair) reports expected_type, else fail with context."""
    assert rows, f"No {context} found for {keyspace}.{table}"
    for row in rows:
        assert row["repair_type"] == expected_type, (
            f"{context} for {keyspace}.{table} reports repair_type {row['repair_type']}, " f"expected {expected_type}"
        )
