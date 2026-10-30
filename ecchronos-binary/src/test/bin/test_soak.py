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

"""Long-running multi-interval soak test.

Brings up the standard 4-node / 2-DC Cassandra cluster and a single ecChronos agent,
creates a dedicated 'soak' keyspace with two populated tables and one intentionally
empty table, and gives every soak table a short repair interval (default 10 minutes,
clamped up to a 2-minute hard floor by build_soak_schedule_keyspaces). It then runs for a
configurable duration (default 1h) that spans several repair intervals.

Primary assertion (authoritative): the REST /schedules endpoint reports a 'lastRepaired'
timestamp that advances for every soak table — including the empty one — across
successive intervals, and no schedule gets stuck in a non-progressing error state.

Supporting telemetry (best-effort): the Prometheus metrics endpoint is scraped each
interval and logged, and used as a soft cross-check that repair activity is happening.

Tuning (environment variables):
  SOAK_DURATION_MINUTES          total run time (default 60)
  SOAK_REPAIR_INTERVAL_MINUTES   per-table repair interval (default 10, clamped to a 2m floor)
  SOAK_CHURN_ROWS_PER_CYCLE      rows written to each populated table per poll (default 500)

The test is opt-in: it is only collected when SOAK_ENABLED=true, so it never runs as part
of the normal test suite. The dedicated 'local-soak-tests' Maven profile sets this.
"""

import logging
import os
import time
from dataclasses import dataclass

import pytest

import global_variables as global_vars
from ecc_config import (
    EcchronosConfig,
    SOAK_KEYSPACE,
    SOAK_POPULATED_TABLES,
    SOAK_EMPTY_TABLE,
    SOAK_TABLES,
    SOAK_DEFAULT_INTERVAL_MINUTES,
    SOAK_MIN_INTERVAL_MINUTES,
    build_soak_schedule_keyspaces,
)
from conftest import (
    soak_schedules_by_table,
    get_metrics_text,
    parse_metric_samples,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    os.environ.get("SOAK_ENABLED", "false").lower() != "true",
    reason="Soak test is opt-in; set SOAK_ENABLED=true (or use the local-soak-tests profile) to run it.",
)


def _int_env(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; falling back to %s", name, os.environ.get(name), default)
        return default


SOAK_DURATION_MINUTES = _int_env("SOAK_DURATION_MINUTES", 60)
# Default interval is SOAK_DEFAULT_INTERVAL_MINUTES (10m); overrides are clamped up to the
# SOAK_MIN_INTERVAL_MINUTES hard floor (2m) so a cycle still has time to complete.
SOAK_REPAIR_INTERVAL_MINUTES = max(
    _int_env("SOAK_REPAIR_INTERVAL_MINUTES", SOAK_DEFAULT_INTERVAL_MINUTES),
    SOAK_MIN_INTERVAL_MINUTES,
)
SOAK_CHURN_ROWS_PER_CYCLE = _int_env("SOAK_CHURN_ROWS_PER_CYCLE", 500)

# Poll the scheduler view a few times per interval so progress is observed promptly.
POLLS_PER_INTERVAL = 4

# A schedule's lastRepaired need not advance every single interval (locks, DC ordering),
# but over this many intervals every table must have advanced at least once; otherwise it
# is genuinely stuck. Kept small so a real stall fails fast rather than after the full run.
MAX_INTERVALS_WITHOUT_PROGRESS = 2

# Repair cannot have started before the agent connected; -1 means "never repaired yet".
NEVER_REPAIRED = -1

# Schedule statuses, mirroring core ScheduledRepairJobView.Status (serialized as the enum
# name over REST): COMPLETED, ON_TIME, LATE, OVERDUE, BLOCKED. (ERROR/RUNNING/WARNING are
# NOT schedule statuses - ERROR belongs to on-demand jobs.)
#
# BLOCKED is terminal-bad: the schedule cannot proceed, so fail immediately.
TERMINAL_BAD_STATUSES = {"BLOCKED"}
# OVERDUE (exceeded the window) and LATE (running behind) mean the table is not keeping up.
# A brief appearance is tolerable, but a table that stays in these without ever reaching
# COMPLETED/ON_TIME across several intervals is falling behind - the direct symptom of the
# scheduler not completing in time.
NON_COMPLETING_STATUSES = {"OVERDUE", "LATE"}
# Statuses that indicate the table is keeping up (resets the non-completing counter).
HEALTHY_STATUSES = {"COMPLETED", "ON_TIME"}
# Number of consecutive intervals a table may remain non-completing (no healthy status
# observed) before the run fails.
MAX_INTERVALS_NON_COMPLETING = 2


def _create_soak_schema(cluster):
    """Create the soak keyspace and tables. table_empty is deliberately never written to."""
    # RF mirrors the existing 'test' keyspace so repairs exercise cross-DC streaming.
    cluster.run_cql(
        f"CREATE KEYSPACE IF NOT EXISTS {SOAK_KEYSPACE} WITH replication = "
        "{'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1};"
    )
    for table in SOAK_TABLES:
        cluster.run_cql(
            f"CREATE TABLE IF NOT EXISTS {SOAK_KEYSPACE}.{table} "
            "(key1 text, key2 int, value int, PRIMARY KEY(key1, key2));"
        )
    logger.info(
        "Created soak schema: keyspace=%s populated=%s empty=%s",
        SOAK_KEYSPACE,
        SOAK_POPULATED_TABLES,
        SOAK_EMPTY_TABLE,
    )


def _write_churn(cluster, cycle, rows=SOAK_CHURN_ROWS_PER_CYCLE):
    """Write a batch of rows to each populated table to create data for repair to reconcile.

    Keys are namespaced by cycle so each interval introduces genuinely new data rather than
    overwriting the same rows. The empty table is intentionally skipped.
    """
    for table in SOAK_POPULATED_TABLES:
        statements = [
            f"INSERT INTO {SOAK_KEYSPACE}.{table} (key1, key2, value) " f"VALUES ('cycle{cycle}', {i}, {i});"
            for i in range(rows)
        ]
        # Batch into a single cqlsh invocation to keep churn cheap relative to the interval.
        cluster.run_cql("\n".join(statements))
    logger.info("Churn cycle %s: wrote %s rows to each of %s", cycle, rows, SOAK_POPULATED_TABLES)


def _max_last_repaired(schedules):
    """Highest lastRepaired across the per-node schedules for a table (-1 if none)."""
    values = [s.last_repaired_at_in_ms for s in schedules if s.last_repaired_at_in_ms != NEVER_REPAIRED]
    return max(values) if values else NEVER_REPAIRED


def _check_status_and_is_healthy(table, schedules):
    """Fail immediately on a terminal BLOCKED status; return True if any node is healthy.

    'Healthy' means at least one node's schedule is COMPLETED or ON_TIME (the table is
    keeping up). OVERDUE/LATE are not failed on a single observation - a table may briefly
    pass through them - but the caller tracks how long a table stays non-completing and
    fails if that persists across intervals (see MAX_INTERVALS_NON_COMPLETING).
    """
    healthy = False
    for schedule in schedules:
        assert schedule.status not in TERMINAL_BAD_STATUSES, (
            f"Soak table {SOAK_KEYSPACE}.{table} schedule on node {schedule.node_id} "
            f"is {schedule.status}; the schedule is blocked and cannot proceed."
        )
        if schedule.status in HEALTHY_STATUSES:
            healthy = True
    return healthy


@dataclass
class _StallCounts:
    """Per-table stall counters evaluated at an interval boundary."""

    intervals_without_progress: int
    intervals_non_completing: int


class _TableState:
    """Tracks one soak table's lastRepaired progress and status health across intervals.

    Two independent guards:
      * progress: lastRepaired must advance; 'intervals_without_progress' counts consecutive
        intervals with no advance.
      * health: a healthy status (COMPLETED/ON_TIME) must be seen; 'intervals_non_completing'
        counts consecutive intervals where only OVERDUE/LATE were observed.
    """

    def __init__(self, baseline_last_repaired):
        self.last_repaired = baseline_last_repaired
        self.advanced_at_least_once = False
        self._intervals_without_progress = 0
        self._intervals_non_completing = 0
        self._progressed_this_interval = False
        self._healthy_this_interval = False

    def start_interval(self):
        self._progressed_this_interval = False
        self._healthy_this_interval = False

    def observe(self, table, current_last_repaired, healthy):
        """Record a poll: update lastRepaired progress and whether the table looked healthy."""
        if current_last_repaired > self.last_repaired:
            logger.info(
                "Progress: %s.%s lastRepaired %s -> %s",
                SOAK_KEYSPACE,
                table,
                self.last_repaired,
                current_last_repaired,
            )
            self.last_repaired = current_last_repaired
            self._progressed_this_interval = True
            self.advanced_at_least_once = True
        if healthy:
            self._healthy_this_interval = True

    def close_interval(self):
        """Advance the per-interval stall counters and return the current counts."""
        if self._progressed_this_interval:
            self._intervals_without_progress = 0
        else:
            self._intervals_without_progress += 1
        if self._healthy_this_interval:
            self._intervals_non_completing = 0
        else:
            self._intervals_non_completing += 1
        return _StallCounts(self._intervals_without_progress, self._intervals_non_completing)


def _log_metrics_telemetry(interval_index):
    """Best-effort supporting telemetry: scrape and log a couple of scheduler metrics."""
    metrics_text = get_metrics_text()
    if metrics_text is None:
        logger.info("[interval %s] metrics endpoint unavailable (telemetry only)", interval_index)
        return
    # These names are logged as telemetry; their absence is not fatal, so we simply report
    # whatever is present. Names below are the exact Prometheus-exposition forms, verified
    # against a running 5.0.4 agent. Per-table repair metrics are unprefixed (the default
    # statistics.prefix is ''); scheduler metrics carry the 'ecc_scheduler_' prefix. The
    # repaired_ratio gauges exist from startup; the ecc_scheduler_* and *_repair_sessions_*
    # metrics only register once a repair has run.
    candidate_metrics = (
        "repaired_ratio",
        "node_repaired_ratio",
        "repair_sessions_seconds_count",
        "node_repair_sessions_seconds_count",
        "ecc_scheduler_refreshed_jobs_count",
        "ecc_scheduler_pass_within_window_total",
        "ecc_scheduler_lock_success_total",
    )
    for metric in candidate_metrics:
        samples = parse_metric_samples(metrics_text, metric)
        if samples:
            logger.info("[interval %s] telemetry %s -> %s", interval_index, metric, samples)


@pytest.mark.dependency(name="test_soak_install_cluster")
def test_soak_install_cluster(install_cassandra_cluster):
    assert install_cassandra_cluster.verify_node_count(4)
    _create_soak_schema(install_cassandra_cluster)


@pytest.mark.dependency(name="test_soak_install_ecchronos", depends=["test_soak_install_cluster"])
def test_soak_install_ecchronos(install_cassandra_cluster, test_environment):
    config = EcchronosConfig(
        soak_keyspaces=build_soak_schedule_keyspaces(SOAK_REPAIR_INTERVAL_MINUTES),
        debug_logging=True,
    )
    test_environment.start_ecchronos(
        cassandra_network=install_cassandra_cluster.network,
        ecchronos_config=config,
    )
    test_environment.wait_for_ecchronos_ready()

    # Sanity: all soak tables (including the empty one) must be registered as schedules.
    schedules = soak_schedules_by_table(keyspace=SOAK_KEYSPACE)
    for table in SOAK_TABLES:
        assert (
            SOAK_KEYSPACE,
            table,
        ) in schedules, f"Expected a schedule for {SOAK_KEYSPACE}.{table}; found {sorted(schedules.keys())}"


def _collect_cassandra_logs(cluster):
    """Best-effort collection of each node's system.log and debug.log as host artifacts."""
    destination = os.path.join(global_vars.PROJECT_BUILD_DIRECTORY, "test", "soak-logs", "cassandra")
    try:
        cluster.collect_node_logs(destination)
        logger.info("Collected Cassandra logs under %s", destination)
    except Exception as exc:  # noqa: BLE001  # pylint: disable=broad-except
        # Log collection must never fail the test.
        logger.error("Failed to collect Cassandra logs: %s", exc)


@pytest.mark.dependency(name="test_soak_multi_interval", depends=["test_soak_install_ecchronos"])
def test_soak_multi_interval(install_cassandra_cluster):
    """Run for several intervals, asserting every soak table's lastRepaired advances."""
    try:
        _run_soak(install_cassandra_cluster)
    finally:
        # Always capture Cassandra logs, whether the soak passed or failed, since the
        # node containers do not bind-mount their logs and are torn down afterwards.
        _collect_cassandra_logs(install_cassandra_cluster)


def _run_soak(install_cassandra_cluster):
    interval_secs = SOAK_REPAIR_INTERVAL_MINUTES * 60
    poll_secs = max(interval_secs // POLLS_PER_INTERVAL, 1)
    total_intervals = max(SOAK_DURATION_MINUTES // SOAK_REPAIR_INTERVAL_MINUTES, 1)
    deadline = time.time() + SOAK_DURATION_MINUTES * 60

    logger.info(
        "Starting soak: duration=%smin interval=%smin (~%s intervals), churn=%s rows/cycle",
        SOAK_DURATION_MINUTES,
        SOAK_REPAIR_INTERVAL_MINUTES,
        total_intervals,
        SOAK_CHURN_ROWS_PER_CYCLE,
    )

    # Baseline lastRepaired per table before we start churning.
    baseline = {
        table: _max_last_repaired(schedules)
        for (ks, table), schedules in soak_schedules_by_table(keyspace=SOAK_KEYSPACE).items()
    }
    states = {table: _TableState(baseline.get(table, NEVER_REPAIRED)) for table in SOAK_TABLES}

    cycle = 0
    interval_index = 0
    next_interval_boundary = time.time() + interval_secs

    while time.time() < deadline:
        cycle += 1
        _write_churn(install_cassandra_cluster, cycle)
        for state in states.values():
            state.start_interval()

        # Poll several times within the interval for prompt observation of progress/status.
        while time.time() < next_interval_boundary and time.time() < deadline:
            schedules_by_table = soak_schedules_by_table(keyspace=SOAK_KEYSPACE)
            for table in SOAK_TABLES:
                schedules = schedules_by_table.get((SOAK_KEYSPACE, table), [])
                assert schedules, f"Lost schedule for {SOAK_KEYSPACE}.{table} during soak run"
                # Fails immediately on BLOCKED; reports whether any node is healthy now.
                healthy = _check_status_and_is_healthy(table, schedules)
                states[table].observe(table, _max_last_repaired(schedules), healthy)
            time.sleep(poll_secs)

        # Interval boundary crossed: evaluate per-table progress and sustained health.
        interval_index += 1
        _log_metrics_telemetry(interval_index)
        _evaluate_interval_boundary(states)
        next_interval_boundary += interval_secs

    # Final assertion: every soak table - including the intentionally empty one - must have
    # had its lastRepaired advance beyond the baseline at least once during the run.
    for table in SOAK_TABLES:
        assert states[table].advanced_at_least_once, (
            f"Soak table {SOAK_KEYSPACE}.{table} never advanced its lastRepaired during the "
            f"{SOAK_DURATION_MINUTES}min run; expected at least one repair cycle to complete."
        )
    logger.info(
        "Soak completed: all soak tables advanced. Final lastRepaired: %s",
        {table: states[table].last_repaired for table in SOAK_TABLES},
    )


def _evaluate_interval_boundary(states):
    """At an interval boundary, advance each table's stall counters and assert the limits."""
    for table in SOAK_TABLES:
        stalled = states[table].close_interval()
        assert stalled.intervals_without_progress <= MAX_INTERVALS_WITHOUT_PROGRESS, (
            f"Soak table {SOAK_KEYSPACE}.{table} lastRepaired did not advance for "
            f"{stalled.intervals_without_progress} intervals "
            f"(limit {MAX_INTERVALS_WITHOUT_PROGRESS}); the scheduler is falling behind."
        )
        assert stalled.intervals_non_completing <= MAX_INTERVALS_NON_COMPLETING, (
            f"Soak table {SOAK_KEYSPACE}.{table} stayed non-completing (OVERDUE/LATE, never "
            f"COMPLETED/ON_TIME) for {stalled.intervals_non_completing} intervals "
            f"(limit {MAX_INTERVALS_NON_COMPLETING}); the scheduler is not completing in time."
        )
