/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.core.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.core.utils.*;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TestEccRepairHistory extends AbstractCassandraTest
{
    private static final String COLUMN_TABLE_ID = "table_id";
    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_REPAIR_ID = "repair_id";
    private static final String COLUMN_JOB_ID = "job_id";
    private static final String COLUMN_COORDINATOR_ID = "coordinator_id";
    private static final String COLUMN_RANGE_BEGIN = "range_begin";
    private static final String COLUMN_RANGE_END = "range_end";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_STARTED_AT = "started_at";
    private static final String COLUMN_FINISHED_AT = "finished_at";

    @Parameterized.Parameters
    public static Collection<String> keyspaceNames()
    {
        return Arrays.asList("ecchronos", "anotherkeyspace");
    }

    @Parameterized.Parameter
    public String keyspaceName;

    private static UUID tableId;

    private static UUID localId;
    private static Node mockLocalNode;
    private static ReplicationState mockReplicationState;

    private static EccRepairHistory repairHistory;
    private static RepairHistoryProvider repairHistoryProvider;

    private static TableReference tableReference;

    private static PreparedStatement iterateStatement;

    @Parameterized.BeforeParam
    public static void setup(String keyspaceName)
    {
        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}",
                keyspaceName));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.repair_history(\n"
                + "  table_id uuid,\n"
                + "  node_id uuid,\n"
                + "  repair_id timeuuid,\n"
                + "  job_id uuid,\n"
                + "  coordinator_id uuid,\n"
                + "  range_begin text,\n"
                + "  range_end text,\n"
                + "  status text,\n"
                + "  started_at timestamp,\n"
                + "  finished_at timestamp,\n"
                + "  PRIMARY KEY((table_id,node_id), repair_id)\n"
                + ") WITH CLUSTERING ORDER BY (repair_id DESC)", keyspaceName));

        mockReplicationState = mock(ReplicationState.class);
        localId = myCluster.getMetadata().getAllHosts().iterator().next().getHostId();
        mockLocalNode = mockNode(localId);

        TableReferenceFactory tableReferenceFactory = new TableReferenceFactoryImpl(myCluster.getMetadata());

        EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                .withLocalNode(mockLocalNode)
                .withLookbackTime(30, TimeUnit.DAYS)
                .withSession(mySession)
                .withKeyspace(keyspaceName)
                .withStatementDecorator(s -> s)
                .withReplicationState(mockReplicationState)
                .build();

        repairHistory = eccRepairHistory;
        repairHistoryProvider = eccRepairHistory;

        tableReference = tableReferenceFactory.forTable(keyspaceName, "repair_history");
        tableId = myCluster.getMetadata().getKeyspace(keyspaceName).getTable("repair_history").getId();

        iterateStatement = mySession.prepare(QueryBuilder.select()
                .from(keyspaceName, "repair_history")
                .where(eq(COLUMN_TABLE_ID, tableId))
                .and(eq(COLUMN_NODE_ID, bindMarker()))
                .and(eq(COLUMN_REPAIR_ID, bindMarker())));
    }

    @After
    public void cleanup()
    {
        mySession.execute(String.format("TRUNCATE %s.repair_history", keyspaceName));
    }

    @Test
    public void testStartAndFinishSession()
    {
        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());
        withKnownRange(range, participants);

        RepairHistory.RepairSession repairSession = repairHistory.newSession(tableReference, jobId, range,
                participants);

        repairSession.start();
        assertCorrectStart(repairSession, jobId, range, participants);

        repairSession.finish(RepairStatus.SUCCESS);
        assertCorrectFinish(repairSession, jobId, range, participants);
    }

    @Test
    public void testStartSessionWithUnknownRange()
    {
        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());

        RepairHistory.RepairSession repairSession = repairHistory
                .newSession(tableReference, jobId, range, participants);
        repairSession.start();

        assertThat(repairSession).isInstanceOf(RepairHistory.NoOpRepairSession.class);
    }

    @Test
    public void testStartAndFailSession()
    {
        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());
        withKnownRange(range, participants);

        RepairHistory.RepairSession repairSession = repairHistory
                .newSession(tableReference, jobId, range, participants);
        repairSession.start();

        assertCorrectStart(repairSession, jobId, range, participants);

        repairSession.finish(RepairStatus.FAILED);

        assertFailedFinish(repairSession, jobId, range, participants);
    }

    @Test
    public void testInsertAndIterate()
    {
        long from = System.currentTimeMillis();

        // Assert that history is empty
        assertThat(repairHistoryProvider.iterate(tableReference, from + 5000, from, Predicates.alwaysTrue())).toIterable().isEmpty();

        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());
        withKnownRange(range, participants);

        RepairHistory.RepairSession repairSession = repairHistory
                .newSession(tableReference, jobId, range, participants);
        repairSession.start();

        long to = System.currentTimeMillis();

        // Assert that we have a started session
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(tableReference, to, from, Predicates.alwaysTrue());
        assertThat(repairEntryIterator.hasNext()).isTrue();
        RepairEntry repairEntry = repairEntryIterator.next();
        assertThat(repairEntry.getParticipants()).isEqualTo(participants);
        assertThat(repairEntry.getStatus()).isEqualTo(RepairStatus.STARTED);
        assertThat(repairEntry.getRange()).isEqualTo(range);
        assertThat(repairEntry.getStartedAt()).isBetween(from, to);
        assertThat(repairEntryIterator.hasNext()).isFalse();

        repairSession.finish(RepairStatus.SUCCESS);

        // Assert that the session has finished
        repairEntryIterator = repairHistoryProvider.iterate(tableReference, to, from, Predicates.alwaysTrue());
        assertThat(repairEntryIterator.hasNext()).isTrue();
        repairEntry = repairEntryIterator.next();
        assertThat(repairEntry.getParticipants()).isEqualTo(participants);
        assertThat(repairEntry.getStatus()).isEqualTo(RepairStatus.SUCCESS);
        assertThat(repairEntry.getRange()).isEqualTo(range);
        assertThat(repairEntry.getStartedAt()).isBetween(from, to);
        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testInsertAndIterateTwoEntries()
    {
        long from = System.currentTimeMillis();

        // Assert that history is empty
        assertThat(repairHistoryProvider.iterate(tableReference, from + 5000, from, Predicates.alwaysTrue())).toIterable().isEmpty();;

        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());
        withKnownRange(range, participants);
        withKnownRange(range2, participants);

        RepairHistory.RepairSession repairSession = repairHistory
                .newSession(tableReference, jobId, range, participants);
        repairSession.start();
        repairSession.finish(RepairStatus.SUCCESS);
        RepairHistory.RepairSession repairSession2 = repairHistory
                .newSession(tableReference, jobId, range2, participants);
        repairSession2.start();
        repairSession2.finish(RepairStatus.SUCCESS);

        long to = System.currentTimeMillis();

        // Assert that the session has finished
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(tableReference, to, from,
                (repairEntry) -> RepairStatus.SUCCESS == repairEntry.getStatus()
                        && repairEntry.getStartedAt() >= from
                        && repairEntry.getStartedAt() <= to);

        List<RepairEntry> repairEntries = Lists.newArrayList(repairEntryIterator);
        assertThat(repairEntries).hasSize(2);
        // Range order is reversed as table is sorted in a descending order
        assertThat(repairEntries.get(0).getRange()).isEqualTo(range2);
        assertThat(repairEntries.get(1).getRange()).isEqualTo(range);
    }

    @Test
    public void testMultipleInvocationsThrowsException()
    {
        UUID jobId = UUID.randomUUID();
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Node> participants = Sets.newHashSet(mockLocalNode, mockNode());
        withKnownRange(range, participants);

        RepairHistory.RepairSession repairSession = repairHistory.newSession(tableReference, jobId, range,
                participants);

        repairSession.start();
        assertCorrectStart(repairSession, jobId, range, participants);

        // We can only start a session once
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(repairSession::start);

        repairSession.finish(RepairStatus.SUCCESS);
        assertCorrectFinish(repairSession, jobId, range, participants);

        // We should not be able to do any more state transitions
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(repairSession::start);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> repairSession.finish(RepairStatus.SUCCESS));
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> repairSession.finish(RepairStatus.FAILED));
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> repairSession.finish(RepairStatus.UNKNOWN));
    }

    private void assertCorrectStart(RepairHistory.RepairSession repairSession, UUID jobId, LongTokenRange range,
            Set<Node> participants)
    {
        for (Node node : participants)
        {
            UUID nodeId = node.getId();
            EccEntry expectedEntry = startedSession(nodeId, internalSession(repairSession).getId(), jobId, range);
            EccEntry actualEntry = fromDb(nodeId, repairSession);
            assertCorrectStartEntry(actualEntry, expectedEntry);
        }
    }

    private void assertFailedFinish(RepairHistory.RepairSession repairSession, UUID jobId, LongTokenRange range,
            Set<Node> participants)
    {
        for (Node node : participants)
        {
            UUID nodeId = node.getId();
            EccEntry base = startedSession(nodeId, internalSession(repairSession).getId(), jobId, range);
            EccEntry expectedEntry = base.failed();
            EccEntry actualEntry = fromDb(nodeId, repairSession);
            assertCorrectEndEntry(actualEntry, expectedEntry);
        }
    }

    private void assertCorrectFinish(RepairHistory.RepairSession repairSession, UUID jobId, LongTokenRange range,
            Set<Node> participants)
    {
        for (Node node : participants)
        {
            UUID nodeId = node.getId();
            EccEntry base = startedSession(nodeId, internalSession(repairSession).getId(), jobId, range);
            EccEntry expectedEntry = base.finished();
            EccEntry actualEntry = fromDb(nodeId, repairSession);
            assertCorrectEndEntry(actualEntry, expectedEntry);
        }
    }

    private void assertCorrectStartEntry(EccEntry actual, EccEntry expected)
    {
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.startedAt).isEqualTo(UUIDs.unixTimestamp(actual.repairId));
    }

    private void assertCorrectEndEntry(EccEntry actual, EccEntry expected)
    {
        assertCorrectStartEntry(actual, expected);
        assertThat(actual.finishedAt).isBetween(actual.startedAt, expected.finishedAt);
    }

    private void withKnownRange(LongTokenRange range, Set<Node> participants)
    {
        when(mockReplicationState.getNodes(tableReference, range)).thenReturn(ImmutableSet.copyOf(participants));
    }

    private static Node mockNode()
    {
        return mockNode(UUID.randomUUID());
    }

    private static Node mockNode(UUID nodeId)
    {
        Node node = mock(Node.class);
        when(node.getId()).thenReturn(nodeId);
        return node;
    }

    private EccEntry startedSession(UUID nodeId, UUID repairId, UUID jobId, LongTokenRange range)
    {
        long startedAt = UUIDs.unixTimestamp(repairId);

        return new EccEntry(tableId, nodeId, repairId, jobId, localId, Long.toString(range.start),
                Long.toString(range.end), "STARTED", startedAt, null);
    }

    private EccEntry fromDb(UUID nodeId, RepairHistory.RepairSession repairSession)
    {
        return fromRow(mySession.execute(iterateStatement.bind(nodeId, internalSession(repairSession).getId())).one());
    }

    private EccEntry fromRow(Row row)
    {
        assertThat(row).isNotNull();
        Long finishedAt = null;
        if (!row.isNull(COLUMN_FINISHED_AT))
        {
            finishedAt = row.getTimestamp(COLUMN_FINISHED_AT).getTime();
        }

        return new EccEntry(row.getUUID(COLUMN_TABLE_ID), row.getUUID(COLUMN_NODE_ID), row.getUUID(COLUMN_REPAIR_ID),
                row.getUUID(COLUMN_JOB_ID), row.getUUID(COLUMN_COORDINATOR_ID), row.getString(COLUMN_RANGE_BEGIN),
                row.getString(COLUMN_RANGE_END), row.getString(COLUMN_STATUS),
                row.getTimestamp(COLUMN_STARTED_AT).getTime(), finishedAt);
    }

    class EccEntry
    {
        final UUID tableId;
        final UUID nodeId;
        final UUID repairId;
        final UUID jobId;
        final UUID coordinatorId;
        final String rangeBegin;
        final String rangeEnd;
        final String status;
        final Long startedAt;
        final Long finishedAt;

        EccEntry(UUID tableId, UUID nodeId, UUID repairId, UUID jobId, UUID coordinatorId, String rangeBegin,
                String rangeEnd, String status, Long startedAt, Long finishedAt)
        {
            this.tableId = tableId;
            this.nodeId = nodeId;
            this.repairId = repairId;
            this.jobId = jobId;
            this.coordinatorId = coordinatorId;
            this.rangeBegin = rangeBegin;
            this.rangeEnd = rangeEnd;
            this.status = status;
            this.startedAt = startedAt;
            this.finishedAt = finishedAt;
        }

        EccEntry failed()
        {
            return finishedWithStatus("FAILED");
        }

        EccEntry finished()
        {
            return finishedWithStatus("SUCCESS");
        }

        EccEntry finishedWithStatus(String status)
        {
            return new EccEntry(tableId, nodeId, repairId, jobId, coordinatorId, rangeBegin, rangeEnd, status,
                    startedAt, System.currentTimeMillis());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            EccEntry eccEntry = (EccEntry) o;
            return Objects.equals(tableId, eccEntry.tableId) &&
                    Objects.equals(nodeId, eccEntry.nodeId) &&
                    Objects.equals(repairId, eccEntry.repairId) &&
                    Objects.equals(jobId, eccEntry.jobId) &&
                    Objects.equals(coordinatorId, eccEntry.coordinatorId) &&
                    Objects.equals(rangeBegin, eccEntry.rangeBegin) &&
                    Objects.equals(rangeEnd, eccEntry.rangeEnd) &&
                    Objects.equals(status, eccEntry.status) &&
                    Objects.equals(startedAt, eccEntry.startedAt);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, nodeId, repairId, jobId, coordinatorId, rangeBegin, rangeEnd, status,
                    startedAt);
        }

        @Override
        public String toString()
        {
            return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", tableId, nodeId, repairId, jobId, coordinatorId,
                    rangeBegin, rangeEnd, status, startedAt, finishedAt);
        }
    }

    private EccRepairHistory.RepairSessionImpl internalSession(RepairHistory.RepairSession repairSession)
    {
        assertThat(repairSession).isInstanceOf(EccRepairHistory.RepairSessionImpl.class);

        return (EccRepairHistory.RepairSessionImpl) repairSession;
    }
}
