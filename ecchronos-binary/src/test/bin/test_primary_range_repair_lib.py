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

"""Unit tests of primary_range_repair_lib. They need no cluster.

The cqlsh outputs below were captured from a Cassandra 5.0 node, unchanged apart from being shortened.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

import primary_range_repair_lib as lib  # pylint: disable=wrong-import-position

HOLDER = "11111111-1111-1111-1111-111111111111"
NODE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
NODE_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

# An LWT statement that was applied, and three of them in one cqlsh call.
APPLIED = "\n [applied]\n-----------\n      True\n"
THREE_APPLIED = "Consistency level set to LOCAL_QUORUM.\nSerial consistency level set to SERIAL.\n" + APPLIED * 3
# An LWT insert that was not applied prints the row that is in the way.
NOT_APPLIED_INSERT = (
    "\n [applied] | resource           | metadata          | node\n"
    "-----------+--------------------+-------------------+--------------------------------------\n"
    "     False | RepairResource-x-1 | {'keyspace': 'k'} | 11111111-1111-1111-1111-111111111111\n"
)
# A conditional delete by someone who is not the holder prints the holder.
NOT_APPLIED_DELETE = (
    "\n [applied] | node\n"
    "-----------+--------------------------------------\n"
    "     False | 11111111-1111-1111-1111-111111111111\n"
)

JSON_ROWS = (
    "Consistency level set to ALL.\n"
    "\n"
    " [json]\n"
    "----------------------------------------------------------------------------------------------\n"
    '     {"coordinator": "172.24.0.2", "range_begin": "0", "range_end": "1", "status": "SUCCESS"}\n'
    ' {"coordinator": "172.24.0.3", "range_begin": "149", "range_end": "150", "status": "SUCCESS"}\n'
    '   {"coordinator": "172.24.0.3", "range_begin": "7", "range_end": "8", "status": "FAILED"}\n'
    "\n"
    "(3 rows)\n"
)


# --- guards for values that end up in CQL -----------------------------------------


def test_require_uuid_accepts_a_uuid():
    assert lib.require_uuid(HOLDER) == HOLDER


@pytest.mark.parametrize("value", ["", "x", HOLDER + "'", "1; DROP TABLE ecchronos.lock", None, HOLDER[:-1]])
def test_require_uuid_rejects_everything_else(value):
    with pytest.raises(ValueError):
        lib.require_uuid(value)


@pytest.mark.parametrize(
    "value",
    [
        "RepairResource-ks.tbl-1",
        "RepairResource-PrimaryRange-datacenter1-1",
        "RepairResource-datacenter2-5",
        f"RepairResource-{NODE_A}-3",
    ],
)
def test_require_resource_accepts_lock_names(value):
    assert lib.require_resource(value) == value


@pytest.mark.parametrize(
    "value",
    ["", "ks.tbl", "RepairResource-ks.tbl", "RepairResource-ks.tbl-x", "RepairResource-a'b-1", "RepairResource-a b-1"],
)
def test_require_resource_rejects_everything_else(value):
    with pytest.raises(ValueError):
        lib.require_resource(value)


# --- parsing cqlsh output ----------------------------------------------------------


def test_parse_json_rows_reads_rows_whatever_the_indentation():
    rows = lib.parse_json_rows(JSON_ROWS)

    assert [row["range_begin"] for row in rows] == ["0", "149", "7"]
    assert rows[0] == {"coordinator": "172.24.0.2", "range_begin": "0", "range_end": "1", "status": "SUCCESS"}


def test_parse_json_rows_of_a_result_without_rows():
    assert lib.parse_json_rows("Consistency level set to ONE.\n\n [json]\n-----\n\n(0 rows)\n") == []


def test_parse_json_rows_fails_loudly_on_a_broken_row():
    with pytest.raises(ValueError):
        lib.parse_json_rows(" {not json}\n")


def test_lwt_results_of_applied_statements():
    assert lib.lwt_results(THREE_APPLIED) == [True, True, True]


def test_lwt_results_of_statements_that_were_not_applied():
    assert lib.lwt_results(NOT_APPLIED_INSERT) == [False]
    assert lib.lwt_results(NOT_APPLIED_DELETE) == [False]


def test_lwt_results_keep_the_order_of_a_mix():
    assert lib.lwt_results(APPLIED + NOT_APPLIED_INSERT + APPLIED) == [True, False, True]


def test_lwt_results_of_output_without_statements():
    assert lib.lwt_results("Consistency level set to ONE.\n") == []


# --- the lock statements ------------------------------------------------------------


def test_lock_insert_script_takes_every_resource_as_a_conditional_insert_with_a_ttl():
    script = lib.lock_insert_script(["RepairResource-ks.tbl-1", "RepairResource-PrimaryRange-dc1-1"], HOLDER, 300)

    assert script.startswith("CONSISTENCY LOCAL_QUORUM; SERIAL CONSISTENCY SERIAL; ")
    assert script.count("IF NOT EXISTS USING TTL 300;") == 2
    assert f"VALUES ('RepairResource-ks.tbl-1', {HOLDER}," in script
    assert f"VALUES ('RepairResource-PrimaryRange-dc1-1', {HOLDER}," in script


def test_lock_delete_script_releases_only_what_the_holder_holds():
    script = lib.lock_delete_script(["RepairResource-ks.tbl-1", "RepairResource-PrimaryRange-dc1-1"], HOLDER)

    assert script.startswith("CONSISTENCY LOCAL_QUORUM; SERIAL CONSISTENCY SERIAL; ")
    assert script.count(f"IF node={HOLDER};") == 2
    assert "WHERE resource='RepairResource-ks.tbl-1'" in script


@pytest.mark.parametrize("builder", [lib.lock_insert_script, lib.lock_delete_script])
def test_lock_scripts_refuse_input_that_is_not_a_resource_or_a_uuid(builder):
    args = (300,) if builder is lib.lock_insert_script else ()
    with pytest.raises(ValueError):
        builder(["RepairResource-a'; DROP TABLE x; --1"], HOLDER, *args)
    with pytest.raises(ValueError):
        builder(["RepairResource-ks.tbl-1"], "not-a-uuid", *args)


# --- lock names ---------------------------------------------------------------------


def test_primary_range_lock_resources_are_the_table_lock_and_one_lock_per_datacenter():
    assert lib.primary_range_lock_resources("ks", "tbl", ["datacenter1", "datacenter2"]) == [
        "RepairResource-ks.tbl-1",
        "RepairResource-PrimaryRange-datacenter1-1",
        "RepairResource-PrimaryRange-datacenter2-1",
    ]


def test_vnode_lock_resources_cover_every_node_and_datacenter_in_every_slot():
    resources = lib.vnode_lock_resources([NODE_A, NODE_B], ["dc1", "dc2"])

    assert len(resources) == lib.MAX_LOCK_SLOTS * 4
    assert len(set(resources)) == len(resources)
    for slot in range(1, lib.MAX_LOCK_SLOTS + 1):
        assert f"RepairResource-{NODE_A}-{slot}" in resources
        assert f"RepairResource-{NODE_B}-{slot}" in resources
        assert f"RepairResource-dc1-{slot}" in resources
        assert f"RepairResource-dc2-{slot}" in resources


def test_primary_range_locks_are_none_of_the_vnode_locks():
    primary_range = set(lib.primary_range_lock_resources("ks", "tbl", ["dc1", "dc2"]))
    vnode = set(lib.vnode_lock_resources([NODE_A, NODE_B], ["dc1", "dc2"]))

    assert not primary_range & vnode


# --- repair history ----------------------------------------------------------------


def _row(repair_type="PRIMARY_RANGE", status="SUCCESS", begin=lib.FULL_RANGE_BEGIN, end=lib.FULL_RANGE_END):
    return {"repair_type": repair_type, "status": status, "range_begin": begin, "range_end": end}


def test_the_ring_marker_is_the_whole_token_space():
    assert lib.FULL_RANGE_BEGIN == "-9223372036854775808"
    assert lib.FULL_RANGE_END == "9223372036854775807"


def test_history_problems_of_correct_history_is_empty():
    assert lib.history_problems({NODE_A: [_row()], NODE_B: [_row(), _row()]}) == []


def test_history_problems_reports_a_node_without_history():
    problems = lib.history_problems({NODE_A: [_row()], NODE_B: []})

    assert problems == [f"node {NODE_B} has no repair history"]


def test_history_problems_reports_a_node_without_a_successful_row():
    problems = lib.history_problems({NODE_A: [_row(status="FAILED")]})

    assert problems == [f"node {NODE_A} has no SUCCESS repair history row"]


def test_history_problems_reports_a_row_of_the_wrong_type():
    problems = lib.history_problems({NODE_A: [_row(), _row(repair_type="VNODE")]})

    assert problems == [f"node {NODE_A} has a row of type VNODE"]


def test_history_problems_reports_a_row_that_is_not_the_ring_marker():
    problems = lib.history_problems({NODE_A: [_row(begin="0", end="100")]})

    assert problems == [f"node {NODE_A} has a row for 0:100"]


def test_history_problems_reports_every_problem_of_every_node_in_node_order():
    problems = lib.history_problems({NODE_B: [], NODE_A: [_row(repair_type=None)]})

    assert problems == [f"node {NODE_A} has a row of type None", f"node {NODE_B} has no repair history"]


# --- what Cassandra recorded --------------------------------------------------------


def test_successful_ranges_by_coordinator_leaves_out_failed_ranges():
    by_coordinator = lib.successful_ranges_by_coordinator(lib.parse_json_rows(JSON_ROWS))

    assert by_coordinator == {"172.24.0.2": {("0", "1")}, "172.24.0.3": {("149", "150")}}


def test_overlapping_ranges_finds_nothing_when_every_coordinator_repaired_its_own_ranges():
    assert lib.overlapping_ranges({"a": {("0", "1")}, "b": {("1", "2")}, "c": {("2", "3")}}) == []


def test_overlapping_ranges_reports_a_range_repaired_by_two_coordinators():
    overlaps = lib.overlapping_ranges({"a": {("0", "1"), ("1", "2")}, "b": {("1", "2")}, "c": {("5", "6")}})

    assert overlaps == [("a", "b", [("1", "2")])]


def test_overlapping_ranges_of_a_single_coordinator():
    assert lib.overlapping_ranges({"a": {("0", "1")}}) == []
    assert lib.overlapping_ranges({}) == []
