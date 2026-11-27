/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.data.repairhistory;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.utils.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.io.IOException;
import java.time.Instant;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryData.Builder;
import static com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryData.copyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRepairHistoryService extends AbstractCassandraTest
{
    private static final String ECCHRONOS_KEYSPACE = "ecchronos";
    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_TABLE_ID = "table_id";
    private static final String COLUMN_REPAIR_ID = "repair_id";
    private static final String COLUMN_COORDINATOR_ID = "coordinator_id";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_REPAIR_TYPE = "repair_type";

    private RepairHistoryService myRepairHistoryService;

    @Mock
    NodeResolver mockNodeResolver;

    @Mock
    ReplicationState mockReplicationState;

    @Before
    public void setup() throws IOException
    {
        AbstractCassandraTest.mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}",
                ECCHRONOS_KEYSPACE));
        String query = String.format(
                "CREATE TABLE IF NOT EXISTS %s.repair_history(" +
                        "table_id UUID, " +
                        "node_id UUID, " +
                        "repair_id timeuuid, " +
                        "job_id UUID, " +
                        "coordinator_id UUID, " +
                        "range_begin text, " +
                        "range_end text, " +
                        "participants set<uuid>, " +
                        "status text, " +
                        "started_at timestamp, " +
                        "finished_at timestamp, " +
                        "repair_type text, " +
                        "PRIMARY KEY((table_id,node_id), repair_id)) " +
                        "WITH CLUSTERING ORDER BY (repair_id DESC);",
                ECCHRONOS_KEYSPACE);
        AbstractCassandraTest.mySession.execute(query);
        myRepairHistoryService = new RepairHistoryService(
                AbstractCassandraTest.mySession,
                mockReplicationState,
                mockNodeResolver,
                1L);
    }

    @After
    public void testCleanup()
    {
        AbstractCassandraTest.mySession.execute(SimpleStatement.newInstance(
                String.format("TRUNCATE %s.%s", ECCHRONOS_KEYSPACE, "repair_history")));
    }

    @Test
    public void testWriteReadUpdateRepairHistoryInfo()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        UUID repairId = Uuids.timeBased();
        UUID coordinatorId = UUID.randomUUID();
        Builder builder = new Builder();

        RepairHistoryData repairHistoryData = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(repairId)
                .withStatus(RepairStatus.STARTED)
                .withStartedAt(Instant.now())
                .withFinishedAt(Instant.now().now())
                .withLookBackTimeInMilliseconds(1)
                .build();

        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData);

        ResultSet result = myRepairHistoryService.getRepairHistoryInfo(repairHistoryData);
        assertNotNull(result);
        Row row = result.one();
        UUID expectedTableId = row.get(COLUMN_TABLE_ID, UUID.class);
        UUID expectedNodeId = row.get(COLUMN_NODE_ID, UUID.class);
        UUID expectedRepairId = row.get(COLUMN_REPAIR_ID, UUID.class);
        UUID expectedCoordinatorId = row.get(COLUMN_COORDINATOR_ID, UUID.class);

        assertEquals(expectedTableId, tableId);
        assertEquals(expectedNodeId, nodeId);
        assertEquals(expectedRepairId, repairId);
        assertEquals(expectedCoordinatorId, null);

        RepairHistoryData updatedRepairHistoryData = copyOf(repairHistoryData)
                .withJobId(UUID.randomUUID())
                .withCoordinatorId(coordinatorId)
                .withRangeBegin("123")
                .withRangeEnd("1000")
                .withParticipants(Collections.EMPTY_SET)
                .withStatus(RepairStatus.SUCCESS)
                .withStartedAt(Instant.now())
                .withFinishedAt(Instant.now().now())
                .withLookBackTimeInMilliseconds(1)
                .build();

        myRepairHistoryService.updateRepairHistoryInfo(updatedRepairHistoryData);

        ResultSet updatedResult = myRepairHistoryService.getRepairHistoryInfo(repairHistoryData);
        Row updatedRow = updatedResult.one();
        expectedCoordinatorId = updatedRow.get(COLUMN_COORDINATOR_ID, UUID.class);
        String expectedStatus = updatedRow.get(COLUMN_STATUS, String.class);
        assertEquals(expectedCoordinatorId, coordinatorId);
        assertEquals(expectedStatus, RepairStatus.SUCCESS.name());
    }

    @Test
    public void testRecordCompletedRepairWritesRowPerReplica()
    {
        UUID tableId = UUID.randomUUID();
        UUID replica1 = UUID.randomUUID();
        UUID replica2 = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getId()).thenReturn(tableId);

        long startedAt = System.currentTimeMillis() - 1000;
        long finishedAt = System.currentTimeMillis();

        myRepairHistoryService.recordCompletedRepair(tableReference, jobId, List.of(replica1, replica2),
                Collections.emptySet(), RepairType.INCREMENTAL, startedAt, finishedAt, RepairStatus.SUCCESS);

        // A row keyed by each replica's node_id must exist with the incremental repair type and SUCCESS status.
        for (UUID replica : List.of(replica1, replica2))
        {
            ResultSet result = AbstractCassandraTest.mySession.execute(SimpleStatement.newInstance(
                    String.format("SELECT * FROM %s.repair_history WHERE table_id = %s AND node_id = %s",
                            ECCHRONOS_KEYSPACE, tableId, replica)));
            Row row = result.one();
            assertNotNull(row);
            assertEquals(tableId, row.get(COLUMN_TABLE_ID, UUID.class));
            assertEquals(replica, row.get(COLUMN_NODE_ID, UUID.class));
            assertEquals(RepairStatus.SUCCESS.name(), row.get(COLUMN_STATUS, String.class));
            assertEquals(RepairType.INCREMENTAL.name(), row.get(COLUMN_REPAIR_TYPE, String.class));
        }
    }

    @Test
    public void testIterateSkipsPrimaryRangeRowsAndKeepsOtherRows()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getId()).thenReturn(tableId);
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(nodeId);

        // Every range resolves to a replica set, so a row is dropped only if it is skipped on purpose and not because
        // its range is missing from the token metadata.
        // The @Mock fields of this class are not initialised, so use a service built on local mocks.
        DriverNode replica = mock(DriverNode.class);
        ReplicationState replicationState = mock(ReplicationState.class);
        when(replicationState.getNodesClusterWide(any(), any(), any())).thenReturn(ImmutableSet.of(replica));
        RepairHistoryService service = new RepairHistoryService(AbstractCassandraTest.mySession, replicationState,
                mock(NodeResolver.class), 1L);

        insertRow(tableId, nodeId, "-100", "100", RepairStatus.SUCCESS, RepairType.VNODE.name());
        insertRow(tableId, nodeId, "-200", "200", RepairStatus.SUCCESS, RepairType.PRIMARY_RANGE.name());
        insertRow(tableId, nodeId, "-300", "300", RepairStatus.SUCCESS, null);

        long now = System.currentTimeMillis();
        Iterator<RepairEntry> iterator = service.iterate(node, tableReference, now + 60_000,
                now - 60_000, entry -> true);
        List<LongTokenRange> ranges = new ArrayList<>();
        iterator.forEachRemaining(entry -> ranges.add(entry.getRange()));

        assertEquals(2, ranges.size());
        assertTrue(ranges.contains(LongTokenRange.of(-100, 100)));
        assertTrue(ranges.contains(LongTokenRange.of(-300, 300)));
        assertFalse(ranges.contains(LongTokenRange.of(-200, 200)));
    }

    @Test
    public void testLastSuccessfulRepairReturnsLatestSuccessOfTheRequestedType()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getId()).thenReturn(tableId);
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(nodeId);

        Instant older = Instant.now().minusSeconds(600);
        Instant newer = Instant.now().minusSeconds(300);
        Instant newest = Instant.now().minusSeconds(100);
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.PRIMARY_RANGE.name(), older);
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.PRIMARY_RANGE.name(), newer);
        // Not counted: failed, a different type, and a type-less (legacy) row, all newer than the answer.
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.FAILED, RepairType.PRIMARY_RANGE.name(), newest);
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.VNODE.name(), newest);
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.INCREMENTAL.name(), newest);
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, null, newest);

        long now = System.currentTimeMillis();
        OptionalLong result = myRepairHistoryService.lastSuccessfulRepair(node, tableReference,
                RepairType.PRIMARY_RANGE, now - 3_600_000L, now + 60_000L);

        assertTrue(result.isPresent());
        assertEquals(newer.toEpochMilli(), result.getAsLong());
    }

    @Test
    public void testLastSuccessfulRepairIsEmptyWithoutAMatchingRow()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getId()).thenReturn(tableId);
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(nodeId);

        insertRow(tableId, nodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.VNODE.name());
        insertRow(tableId, nodeId, "-1", "1", RepairStatus.FAILED, RepairType.PRIMARY_RANGE.name());

        long now = System.currentTimeMillis();
        assertFalse(myRepairHistoryService.lastSuccessfulRepair(node, tableReference, RepairType.PRIMARY_RANGE,
                now - 3_600_000L, now + 60_000L).isPresent());
    }

    @Test
    public void testLastSuccessfulRepairOnlyLooksAtTheGivenNodeAndTable()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();
        UUID otherNodeId = UUID.randomUUID();
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getId()).thenReturn(tableId);
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(nodeId);

        insertRow(tableId, otherNodeId, "-1", "1", RepairStatus.SUCCESS, RepairType.PRIMARY_RANGE.name());

        long now = System.currentTimeMillis();
        assertFalse(myRepairHistoryService.lastSuccessfulRepair(node, tableReference, RepairType.PRIMARY_RANGE,
                now - 3_600_000L, now + 60_000L).isPresent());
    }

    private void insertRow(final UUID tableId, final UUID nodeId, final String rangeBegin, final String rangeEnd,
            final RepairStatus status, final String repairType)
    {
        insertRow(tableId, nodeId, rangeBegin, rangeEnd, status, repairType, Instant.now());
    }

    private void insertRow(final UUID tableId, final UUID nodeId, final String rangeBegin, final String rangeEnd,
            final RepairStatus status, final String repairType, final Instant finishedAt)
    {
        Instant startedAt = finishedAt.minusSeconds(5);
        AbstractCassandraTest.mySession.execute(SimpleStatement.newInstance(
                "INSERT INTO ecchronos.repair_history (table_id, node_id, repair_id, job_id, coordinator_id, "
                        + "range_begin, range_end, participants, status, started_at, finished_at, repair_type) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableId, nodeId, Uuids.startOf(startedAt.toEpochMilli()), UUID.randomUUID(), nodeId, rangeBegin,
                rangeEnd, Collections.emptySet(), status.name(), startedAt, finishedAt, repairType));
    }

    @Test
    public void testGetRepairHistoryByTimePeriod()
    {
        // Insert multiple repair history data entries
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        Builder builder = new Builder();

        Instant now = Instant.now();
        Instant oneHourAgo = now.minusSeconds(3600);
        Instant oneHourLater = now.plusSeconds(3600);

        // Entry 1 (within range)
        RepairHistoryData repairHistoryData1 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.STARTED)
                .withStartedAt(oneHourAgo) // 1 hour ago
                .withFinishedAt(now) // Now
                .withLookBackTimeInMilliseconds(1)
                .build();

        // Entry 2 (within range)
        RepairHistoryData repairHistoryData2 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.SUCCESS)
                .withStartedAt(now.minusSeconds(600)) // 10 minutes ago
                .withFinishedAt(now.plusSeconds(600)) // 10 minutes in the future
                .withLookBackTimeInMilliseconds(1)
                .build();

        // Insert data into the repair history table
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData1);
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData2);

        // Fetch the data by time period (including the last hour)
        RepairHistoryData queryData = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withStartedAt(oneHourAgo) // Start of time range (1 hour ago)
                .withFinishedAt(oneHourLater) // End of time range (1 hour in the future)
                .build();

        List<RepairHistoryData> result = myRepairHistoryService.getRepairHistoryByTimePeriod(queryData);

        // Verify the fetched data is correct and within the expected range
        assertNotNull(result);

        // There should be at least two rows matching the time range
        assertEquals(2, result.size());

        for (RepairHistoryData repairHistoryData : result)
        {
            UUID expectedTableId = repairHistoryData.getTableId();
            UUID expectedNodeId = repairHistoryData.getNodeId();

            assertEquals(expectedTableId, tableId);
            assertEquals(expectedNodeId, nodeId);
        }
    }

    @Test
    public void testGetRepairHistoryByStatus()
    {
        // Insert multiple repair history data entries
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        Builder builder = new Builder();

        // Entry 1 (status: SUCCESS)
        RepairHistoryData repairHistoryData1 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.SUCCESS)
                .withStartedAt(Instant.now().minusSeconds(3600)) // 1 hour ago
                .withFinishedAt(Instant.now())
                .withLookBackTimeInMilliseconds(1)
                .build();

        // Entry 2 (status: FAILED)
        RepairHistoryData repairHistoryData2 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.FAILED)
                .withStartedAt(Instant.now().minusSeconds(7200)) // 2 hours ago
                .withFinishedAt(Instant.now().minusSeconds(3600)) // 1 hour ago
                .build();

        // Insert data into the repair history table
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData1);
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData2);

        RepairHistoryData queryData = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withStatus(RepairStatus.SUCCESS)
                .build();

        List<RepairHistoryData> result = myRepairHistoryService.getRepairHistoryByStatus(queryData);

        // Verify the fetched data is correct and matches the expected status
        assertNotNull(result);
        assertEquals(1, result.size());

        for (RepairHistoryData repairHistoryData : result)
        {
            UUID expectedTableId = repairHistoryData.getTableId();
            UUID expectedNodeId = repairHistoryData.getNodeId();
            String status = repairHistoryData.getStatus().name();

            assertEquals(expectedTableId, tableId);
            assertEquals(expectedNodeId, nodeId);
            assertEquals(status, RepairStatus.SUCCESS.name());
        }
    }

    @Test
    public void testGetRepairHistoryByStatusNoMatches()
    {
        // Insert a repair history entry with a different status
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        Builder builder = new Builder();

        // Entry (status: IN_PROGRESS)
        RepairHistoryData repairHistoryData = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.STARTED)
                .withStartedAt(Instant.now().minusSeconds(3600)) // 1 hour ago
                .withFinishedAt(Instant.now()) // Now
                .withLookBackTimeInMilliseconds(1)
                .build();

        // Insert data into the repair history table
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData);

        // Fetch the data by a different status
        RepairHistoryData queryData = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withStatus(RepairStatus.SUCCESS)
                .withLookBackTimeInMilliseconds(1)
                .build();

        List<RepairHistoryData> result = myRepairHistoryService.getRepairHistoryByStatus(queryData);

        // Verify that no rows match the SUCCESS status
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testGetRepairHistoryByLookBackTime()
    {
        UUID tableId = UUID.randomUUID();
        UUID nodeId = UUID.randomUUID();

        Builder builder = new Builder();

        Instant now = Instant.now();
        Instant thirtyDaysAgo = now.minusSeconds(30 * 24 * 60 * 60); // 30 days ago
        Instant oneHourAgo = now.minusSeconds(3600); // 1 hour ago
        Instant twoMonthsAgo = now.minusSeconds(60 * 24 * 60 * 60); // 60 days ago (outside the lookback window)

        System.out.println("Now: " + now);
        System.out.println("Thirty days ago: " + thirtyDaysAgo);
        System.out.println("One hour ago: " + oneHourAgo);
        System.out.println("Two months ago: " + twoMonthsAgo);

        // Entry 1 (within lookback time, 30 days ago)
        RepairHistoryData repairHistoryData1 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.STARTED)
                .withStartedAt(thirtyDaysAgo) // 30 days ago
                .withFinishedAt(now)
                .withLookBackTimeInMilliseconds(30 * 24 * 60 * 60 * 1000L) // 30 days in milliseconds
                .build();

        // Entry 2 (within lookback time, 1 hour ago)
        RepairHistoryData repairHistoryData2 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.SUCCESS)
                .withStartedAt(oneHourAgo) // 1 hour ago
                .withFinishedAt(now)
                .withLookBackTimeInMilliseconds(30 * 24 * 60 * 60 * 1000L) // 30 days in milliseconds
                .build();

        // Entry 3 (outside lookback time, 60 days ago)
        RepairHistoryData repairHistoryData3 = builder.withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(Uuids.timeBased())
                .withStatus(RepairStatus.FAILED)
                .withStartedAt(twoMonthsAgo) // 60 days ago
                .withFinishedAt(now.minusSeconds(60 * 24 * 60 * 60)) // Also 60 days ago
                .withLookBackTimeInMilliseconds(30 * 24 * 60 * 60 * 1000L) // 30 days in milliseconds
                .build();

        // Insert data into the repair history table
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData1);
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData2);
        myRepairHistoryService.insertRepairHistoryInfo(repairHistoryData3);

        // Fetch the data by lookback time (30 days)
        RepairHistoryData queryData = builder.withLookBackTimeInMilliseconds(30 * 24 * 60 * 60 * 1000L).build();
        List<RepairHistoryData> result = myRepairHistoryService.getRepairHistoryByLookBackTime(queryData);

        // Verify the fetched data is correct and within the look back time range
        assertNotNull(result);
        assertEquals(2, result.size()); // Should return only two entries

        // Tolerance for comparing timestamps, in milliseconds
        long toleranceMillis = 1;
        for (RepairHistoryData repairHistoryData : result)
        {
            UUID expectedTableId = repairHistoryData.getTableId();
            UUID expectedNodeId = repairHistoryData.getNodeId();

            // Validate table ID and node ID
            assertEquals(expectedTableId, tableId);
            assertEquals(expectedNodeId, nodeId);

            Instant startedAt = repairHistoryData.getStartedAt();

            // Check if startedAt is after or equal to thirtyDaysAgo with a tolerance
            boolean isAfterOrEqual = startedAt.isAfter(thirtyDaysAgo) ||
                    (startedAt.toEpochMilli() >= thirtyDaysAgo.toEpochMilli() - toleranceMillis);

            assertTrue("Expected startedAt " + startedAt + " to be after or equal to " + thirtyDaysAgo, isAfterOrEqual);
        }
    }
}
