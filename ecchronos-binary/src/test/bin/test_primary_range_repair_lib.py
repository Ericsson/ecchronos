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

"""Unit tests of primary_range_repair_lib, the helpers of the PRIMARY_RANGE and ALL_REPLICATED_RANGES integration
tests. They need no cluster.

The cqlsh outputs below were captured from a Cassandra 5.0 node, unchanged apart from being shortened.
"""

import os
import sys
import uuid

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


def test_overlapping_ranges_finds_nothing_when_every_coordinator_repaired_its_own_ranges():
    assert lib.overlapping_ranges({"a": {("0", "1")}, "b": {("1", "2")}, "c": {("2", "3")}}) == []


def test_overlapping_ranges_reports_a_range_repaired_by_two_coordinators():
    overlaps = lib.overlapping_ranges({"a": {("0", "1"), ("1", "2")}, "b": {("1", "2")}, "c": {("5", "6")}})

    assert overlaps == [("a", "b", [("1", "2")])]


def test_overlapping_ranges_of_a_single_coordinator():
    assert lib.overlapping_ranges({"a": {("0", "1")}}) == []
    assert lib.overlapping_ranges({}) == []


# --- history of the other repair type ---------------------------------------------------


def test_history_problems_of_all_replicated_ranges_history_is_empty():
    rows = [_row(repair_type=lib.REPAIR_TYPE_ALL_REPLICATED_RANGES)]

    assert lib.history_problems({NODE_A: rows}, lib.REPAIR_TYPE_ALL_REPLICATED_RANGES) == []


def test_history_problems_does_not_take_a_primary_range_row_for_an_all_replicated_ranges_row():
    rows = [_row(repair_type=lib.REPAIR_TYPE_ALL_REPLICATED_RANGES), _row(repair_type=lib.REPAIR_TYPE_PRIMARY_RANGE)]

    problems = lib.history_problems({NODE_A: rows}, lib.REPAIR_TYPE_ALL_REPLICATED_RANGES)

    assert problems == [f"node {NODE_A} has a row of type PRIMARY_RANGE"]


def test_history_problems_does_not_take_an_all_replicated_ranges_row_for_a_primary_range_row():
    rows = [_row(repair_type=lib.REPAIR_TYPE_ALL_REPLICATED_RANGES)]

    # PRIMARY_RANGE is the default, so the existing callers are unchanged.
    assert lib.history_problems({NODE_A: rows}) == [f"node {NODE_A} has a row of type ALL_REPLICATED_RANGES"]


# --- ids of repair sessions ----------------------------------------------------------


def _timeuuid(epoch_millis):
    """A version 1 UUID for a time, built like Cassandra does for the id of a repair session."""
    ticks = epoch_millis * 10_000 + 0x01B21DD213814000
    return str(
        uuid.UUID(
            fields=(ticks & 0xFFFFFFFF, (ticks >> 32) & 0xFFFF, ((ticks >> 48) & 0x0FFF) | 0x1000, 0x80, 0x01, 0x42)
        )
    )


def test_timeuuid_millis_reads_the_creation_time():
    assert lib.timeuuid_millis(_timeuuid(1_764_000_000_123)) == 1_764_000_000_123


def test_timeuuid_millis_of_a_uuid_generated_by_python():
    before = int(uuid.uuid1().time - 0x01B21DD213814000) // 10_000

    assert lib.timeuuid_millis(str(uuid.uuid1())) >= before


@pytest.mark.parametrize("value", [str(uuid.uuid4()), "x", "", None])
def test_timeuuid_millis_rejects_what_is_not_a_time_based_uuid(value):
    with pytest.raises(ValueError):
        lib.timeuuid_millis(value)


# --- the nodes of the cluster ----------------------------------------------------------

LOCAL_NODE = (
    '{"host_id": "11111111-1111-1111-1111-111111111111", "broadcast_address": "172.24.0.2", "data_center": "dc1"}'
)
PEER_NODES = (
    "Consistency level set to ONE.\n"
    "\n [json]\n---------\n"
    ' {"host_id": "33333333-3333-3333-3333-333333333333", "peer": "172.24.0.4", "data_center": "dc2"}\n'
    ' {"host_id": "22222222-2222-2222-2222-222222222222", "peer": "172.24.0.3", "data_center": "dc2"}\n'
    "\n(2 rows)\n"
)


def test_cluster_nodes_combines_the_local_node_and_the_peers_in_address_order():
    nodes = lib.cluster_nodes(lib.parse_json_rows(LOCAL_NODE), lib.parse_json_rows(PEER_NODES))

    assert nodes == [
        {"host_id": "11111111-1111-1111-1111-111111111111", "address": "172.24.0.2", "datacenter": "dc1"},
        {"host_id": "22222222-2222-2222-2222-222222222222", "address": "172.24.0.3", "datacenter": "dc2"},
        {"host_id": "33333333-3333-3333-3333-333333333333", "address": "172.24.0.4", "datacenter": "dc2"},
    ]


def test_cluster_nodes_refuses_a_host_id_that_is_not_a_uuid():
    with pytest.raises(ValueError):
        lib.cluster_nodes([{"host_id": "x", "broadcast_address": "172.24.0.2", "data_center": "dc1"}], [])


def test_addresses_in_a_datacenter():
    nodes = lib.cluster_nodes(lib.parse_json_rows(LOCAL_NODE), lib.parse_json_rows(PEER_NODES))

    assert lib.addresses_in(nodes, "dc2") == ["172.24.0.3", "172.24.0.4"]
    assert lib.addresses_in(nodes, "dc1") == ["172.24.0.2"]
    assert lib.addresses_in(nodes, "dc3") == []


# --- who repaired which range ---------------------------------------------------------


def _repair(coordinator, begin, end, status="SUCCESS", participants=None):
    return {
        "coordinator": coordinator,
        "range_begin": begin,
        "range_end": end,
        "status": status,
        "participants": participants,
    }


def test_parse_range_reads_what_cassandra_writes():
    assert lib.parse_range("(-2812148611369746005,-2610412061160269928]") == (
        "-2812148611369746005",
        "-2610412061160269928",
    )
    assert lib.parse_range("(5,9]") == ("5", "9")
    assert lib.parse_range("( 5 , 9 ]") == ("5", "9")


@pytest.mark.parametrize("value", ["", "5,9", "[5,9]", "(5,9)", "(a,b]", "(5,]", "(5,9]x", None])
def test_parse_range_refuses_anything_else(value):
    with pytest.raises(ValueError):
        lib.parse_range(value)


def test_parse_ranges_of_a_set_of_range_texts():
    assert lib.parse_ranges(["(0,1]", "(1,2]", "(0,1]"]) == {("0", "1"), ("1", "2")}


def test_parse_ranges_of_no_ranges():
    assert lib.parse_ranges(None) == set()
    assert lib.parse_ranges([]) == set()


PARENT_1 = _timeuuid(1_000)
PARENT_2 = _timeuuid(2_000)
PARENT_3 = _timeuuid(3_000)

# What cqlsh prints for the two queries. A repair has one history row for every session, so one parent can have several.
HISTORY = lib.parse_json_rows(
    "\n [json]\n-----\n"
    f' {{"parent_id": "{PARENT_2}", "coordinator": "172.24.0.3"}}\n'
    f' {{"parent_id": "{PARENT_1}", "coordinator": "172.24.0.2"}}\n'
    f' {{"parent_id": "{PARENT_1}", "coordinator": "172.24.0.2"}}\n'
    f' {{"parent_id": "{PARENT_3}", "coordinator": "172.24.0.2"}}\n'
    "\n(4 rows)\n"
)
PARENTS = lib.parse_json_rows(
    "\n [json]\n-----\n"
    f' {{"parent_id": "{PARENT_1}", "requested_ranges": ["(0,1]", "(1,2]", "(2,3]"], '
    '"successful_ranges": ["(0,1]", "(1,2]"], "options": {"primaryRange": "false", "incremental": "false"}}\n'
    f' {{"parent_id": "{PARENT_2}", "requested_ranges": ["(1,2]"], '
    '"successful_ranges": ["(1,2]"], "options": {"primaryRange": "true"}}\n'
    "\n(2 rows)\n"
)


def test_parent_repairs_joins_the_coordinator_the_ranges_and_the_options_of_every_repair():
    repairs = lib.parent_repairs(HISTORY, PARENTS)

    first = repairs[0]
    assert first["parent_id"] == PARENT_1
    assert first["coordinator"] == "172.24.0.2"
    assert first["started_ms"] == 1_000
    assert first["requested"] == {("0", "1"), ("1", "2"), ("2", "3")}
    assert first["successful"] == {("0", "1"), ("1", "2")}
    assert first["options"] == {"primaryRange": "false", "incremental": "false"}


def test_parent_repairs_are_one_per_repair_however_many_sessions_it_had_and_oldest_first():
    repairs = lib.parent_repairs(HISTORY, PARENTS)

    assert [repair["parent_id"] for repair in repairs] == [PARENT_1, PARENT_2, PARENT_3]
    assert [repair["started_ms"] for repair in repairs] == [1_000, 2_000, 3_000]


def test_a_repair_whose_parent_row_is_not_written_yet_has_no_ranges_and_no_options():
    third = lib.parent_repairs(HISTORY, PARENTS)[2]

    assert third["parent_id"] == PARENT_3
    assert third["requested"] == set()
    assert third["successful"] == set()
    assert third["options"] == {}


def test_a_parent_row_without_ranges_has_no_ranges():
    parents = [{"parent_id": PARENT_1, "requested_ranges": None, "successful_ranges": None, "options": None}]

    history = [row for row in HISTORY if row["parent_id"] == PARENT_1][:1]

    repair = lib.parent_repairs(history, parents)[0]

    assert repair["requested"] == set() and repair["successful"] == set() and repair["options"] == {}


def test_parent_repairs_of_nothing():
    assert lib.parent_repairs([], []) == []


def test_parent_repairs_refuses_a_repair_with_two_coordinators():
    rows = [{"parent_id": PARENT_1, "coordinator": "a"}, {"parent_id": PARENT_1, "coordinator": "b"}]

    with pytest.raises(ValueError):
        lib.parent_repairs(rows, [])


def test_parent_repairs_refuses_a_parent_id_that_is_not_a_uuid():
    with pytest.raises(ValueError):
        lib.parent_repairs([{"parent_id": "1; DROP TABLE x", "coordinator": "a"}], [])


def test_repaired_ranges_by_coordinator_are_the_successful_ranges_of_all_its_repairs():
    repairs = lib.parent_repairs(HISTORY, PARENTS)

    assert lib.repaired_ranges_by_coordinator(repairs) == {
        "172.24.0.2": {("0", "1"), ("1", "2")},
        "172.24.0.3": {("1", "2")},
    }


def test_a_range_that_was_requested_but_not_repaired_does_not_count():
    repairs = lib.parent_repairs(HISTORY, PARENTS)

    assert ("2", "3") not in lib.repaired_ranges_by_coordinator(repairs)["172.24.0.2"]


def test_coordinators_of_ranges_lists_every_coordinator_of_a_range():
    ranges = {"a": {("0", "1"), ("1", "2")}, "b": {("1", "2")}, "c": {("1", "2")}}

    assert lib.coordinators_of_ranges(ranges) == {("0", "1"): {"a"}, ("1", "2"): {"a", "b", "c"}}


def test_coordinators_of_ranges_of_nothing():
    assert lib.coordinators_of_ranges({}) == {}


def test_primary_range_repair_has_one_coordinator_for_every_range_and_the_other_type_has_several():
    primary = {"a": {("0", "1")}, "b": {("1", "2")}}
    replicated = {c: {("0", "1"), ("1", "2")} for c in "abc"}

    assert {len(c) for c in lib.coordinators_of_ranges(primary).values()} == {1}
    assert {len(c) for c in lib.coordinators_of_ranges(replicated).values()} == {3}


def test_repairs_not_started_as_finds_the_repairs_with_other_options():
    repairs = lib.parent_repairs(HISTORY, PARENTS)

    wrong = lib.repairs_not_started_as(repairs, primaryRange="false")

    # The second repair is a primary range repair, the third has no options yet.
    assert [(repair["parent_id"], key, actual) for repair, key, actual in wrong] == [
        (PARENT_2, "primaryRange", "true"),
        (PARENT_3, "primaryRange", None),
    ]


def test_repairs_not_started_as_checks_every_option_asked_for():
    repairs = lib.parent_repairs(HISTORY, PARENTS)[:1]

    assert lib.repairs_not_started_as(repairs, primaryRange="false", incremental="false") == []
    wrong = lib.repairs_not_started_as(repairs, primaryRange="false", incremental="true")
    assert [(key, actual) for _, key, actual in wrong] == [("incremental", "false")]


def test_missing_ranges_are_the_expected_ranges_that_were_not_repaired_in_order():
    # Enough ranges that a set could not return them in order by chance.
    expected = {(str(begin), str(begin + 1)) for begin in range(9, -1, -1)}

    missing = lib.missing_ranges(expected, {("3", "4"), ("7", "8")})

    assert missing == [(str(b), str(b + 1)) for b in (0, 1, 2, 4, 5, 6, 8, 9)]


def test_missing_ranges_ignores_ranges_that_were_not_expected():
    assert lib.missing_ranges({("0", "1")}, {("0", "1"), ("9", "10")}) == []


def test_missing_ranges_of_nothing_expected():
    assert lib.missing_ranges(set(), {("0", "1")}) == []


# --- who took part ------------------------------------------------------------------


def test_rows_without_participant_in_finds_the_sessions_that_left_a_datacenter_out():
    rows = [
        _repair("a", "0", "1", participants=["172.24.0.2", "172.24.0.3"]),
        _repair("a", "1", "2", participants=["172.24.0.2", "172.24.0.5"]),
        _repair("a", "2", "3", participants=None),
    ]

    left_out = lib.rows_without_participant_in(rows, ["172.24.0.3", "172.24.0.4"])

    assert [row["range_begin"] for row in left_out] == ["1", "2"]


def test_rows_without_participant_in_of_no_addresses_finds_every_row():
    rows = [_repair("a", "0", "1", participants=["172.24.0.2"])]

    assert lib.rows_without_participant_in(rows, []) == rows


def test_participants_of_is_every_address_that_took_part():
    rows = [
        _repair("a", "0", "1", participants=["172.24.0.2", "172.24.0.3"]),
        _repair("a", "1", "2", participants=["172.24.0.4"]),
        _repair("a", "2", "3", participants=None),
    ]

    assert lib.participants_of(rows) == {"172.24.0.2", "172.24.0.3", "172.24.0.4"}
    assert lib.participants_of([]) == set()
