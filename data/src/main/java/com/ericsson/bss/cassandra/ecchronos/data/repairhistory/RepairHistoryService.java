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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.history.SessionState;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public final class RepairHistoryService implements RepairHistory, RepairHistoryProvider
{

    private static final Logger LOG = LoggerFactory.getLogger(RepairHistoryService.class);
    private static final String UNIVERSAL_TIMEZONE = "UTC";

    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "repair_history";
    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_TABLE_ID = "table_id";
    private static final String COLUMN_REPAIR_ID = "repair_id";
    private static final String COLUMN_JOB_ID = "job_id";
    private static final String COLUMN_COORDINATOR_ID = "coordinator_id";
    private static final String COLUMN_RANGE_BEGIN = "range_begin";
    private static final String COLUMN_RANGE_END = "range_end";
    private static final String COLUMN_PARTICIPANTS = "participants";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_STARTED_AT = "started_at";
    private static final String COLUMN_FINISHED_AT = "finished_at";

    private final PreparedStatement myCreateStatement;
    private final PreparedStatement myUpdateStatement;
    private final PreparedStatement mySelectStatement;
    private final PreparedStatement myIterateStatement;

    private final CqlSession myCqlSession;
    private final ReplicationState myReplicationState;
    private final NodeResolver myNodeResolver;
    private final long myLookbackTimeInMs;

    public RepairHistoryService(
            final CqlSession cqlSession,
            final ReplicationState replicationState,
            final NodeResolver nodeResolver,
            final long lookbackTimeInMs)
    {
        myLookbackTimeInMs = lookbackTimeInMs;
        myReplicationState = replicationState;
        myNodeResolver = nodeResolver;
        myCqlSession = Preconditions.checkNotNull(cqlSession, "CqlSession cannot be null");
        myCreateStatement = myCqlSession
                .prepare(QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME)
                        .value(COLUMN_TABLE_ID, bindMarker())
                        .value(COLUMN_NODE_ID, bindMarker())
                        .value(COLUMN_REPAIR_ID, bindMarker())
                        .value(COLUMN_JOB_ID, bindMarker())
                        .value(COLUMN_COORDINATOR_ID, bindMarker())
                        .value(COLUMN_RANGE_BEGIN, bindMarker())
                        .value(COLUMN_RANGE_END, bindMarker())
                        .value(COLUMN_PARTICIPANTS, bindMarker())
                        .value(COLUMN_STATUS, bindMarker())
                        .value(COLUMN_STARTED_AT, bindMarker())
                        .value(COLUMN_FINISHED_AT, bindMarker())
                        .build()
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

        myUpdateStatement = myCqlSession
                .prepare(QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME)
                        .setColumn(COLUMN_JOB_ID, bindMarker())
                        .setColumn(COLUMN_COORDINATOR_ID, bindMarker())
                        .setColumn(COLUMN_RANGE_BEGIN, bindMarker())
                        .setColumn(COLUMN_RANGE_END, bindMarker())
                        .setColumn(COLUMN_PARTICIPANTS, bindMarker())
                        .setColumn(COLUMN_STATUS, bindMarker())
                        .setColumn(COLUMN_STARTED_AT, bindMarker())
                        .setColumn(COLUMN_FINISHED_AT, bindMarker())
                        .whereColumn(COLUMN_TABLE_ID)
                        .isEqualTo(bindMarker())
                        .whereColumn(COLUMN_NODE_ID)
                        .isEqualTo(bindMarker())
                        .whereColumn(COLUMN_REPAIR_ID)
                        .isEqualTo(bindMarker())
                        .build()
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
        mySelectStatement = myCqlSession
                .prepare(selectFrom(KEYSPACE_NAME, TABLE_NAME)
                        .columns(COLUMN_NODE_ID, COLUMN_TABLE_ID, COLUMN_REPAIR_ID, COLUMN_JOB_ID, COLUMN_COORDINATOR_ID,
                                COLUMN_RANGE_BEGIN, COLUMN_RANGE_END, COLUMN_PARTICIPANTS, COLUMN_STATUS, COLUMN_STARTED_AT,
                                COLUMN_FINISHED_AT)
                        .whereColumn(COLUMN_TABLE_ID)
                        .isEqualTo(bindMarker())
                        .whereColumn(COLUMN_NODE_ID)
                        .isEqualTo(bindMarker())
                        .build()
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
        myIterateStatement = myCqlSession.prepare(QueryBuilder.selectFrom(KEYSPACE_NAME, TABLE_NAME)
                .columns(COLUMN_STARTED_AT, COLUMN_FINISHED_AT, COLUMN_STATUS, COLUMN_RANGE_BEGIN, COLUMN_RANGE_END)
                .whereColumn(COLUMN_TABLE_ID).isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE_ID).isEqualTo(bindMarker())
                .whereColumn(COLUMN_REPAIR_ID).isGreaterThanOrEqualTo(bindMarker())
                .whereColumn(COLUMN_REPAIR_ID).isLessThanOrEqualTo(bindMarker()).build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE));
    }

    /**
     * Retrieves the repair history entries that fall within the specified time period.
     *
     * This method fetches repair history rows from a Cassandra table and filters them based on the
     * started and finished timestamps in the given time period. The filtering is done using local
     * dates to ignore timezone differences.
     *
     * @param repairHistoryData The data containing the time period to filter the repair history.
     * @return A list of filtered RepairHistoryData objects containing repair history entries.
     */
    public List<RepairHistoryData> getRepairHistoryByTimePeriod(final RepairHistoryData repairHistoryData)
    {
        ResultSet resultSet = getRepairHistoryInfo(repairHistoryData);
        List<Row> rows = StreamSupport.stream(resultSet.spliterator(), false).collect(Collectors.toList());

        Instant queryStartedAt = repairHistoryData.getStartedAt();
        Instant queryFinishedAt = repairHistoryData.getFinishedAt();

        LocalDate queryStartDate = queryStartedAt.atZone(ZoneId.of(UNIVERSAL_TIMEZONE)).toLocalDate();
        LocalDate queryFinishDate = queryFinishedAt.atZone(ZoneId.of(UNIVERSAL_TIMEZONE)).toLocalDate();

        List<RepairHistoryData> filteredHistoryData = rows.stream()
                .filter(row ->
                {
                    // Extract the started and finished timestamps from the row
                    Instant startedAt = row.get(COLUMN_STARTED_AT, Instant.class);
                    Instant finishedAt = row.get(COLUMN_FINISHED_AT, Instant.class);

                    // Convert the row start and finish times to local dates
                    LocalDate rowStartDate = startedAt.atZone(ZoneId.of(UNIVERSAL_TIMEZONE)).toLocalDate();
                    LocalDate rowFinishDate = finishedAt.atZone(ZoneId.of(UNIVERSAL_TIMEZONE)).toLocalDate();

                    LOG.debug("Filtering row: startedAt={}, finishedAt={}, queryStartedAt={}, queryFinishedAt={}",
                            startedAt, finishedAt, queryStartedAt, queryFinishedAt);

                    // Check if the row is within the date range
                    return !rowFinishDate.isBefore(queryStartDate) && !rowStartDate.isAfter(queryFinishDate);
                })
                .map(row -> convertRowToRepairHistoryData(row, repairHistoryData.getLookBackTimeInMilliseconds()))
                .collect(Collectors.toList());

        return filteredHistoryData;
    }

    /**
     * Retrieves the repair history entries that match the specified status.
     *
     * This method fetches all repair history rows from a Cassandra table and filters them
     * based on the provided status in the RepairHistoryData object. The results are returned as a list
     * of filtered RepairHistoryData objects.
     *
     * @param repairHistoryData The data containing the status to filter the repair history.
     * @return A list of filtered RepairHistoryData objects that match the specified status.
     */
    public List<RepairHistoryData> getRepairHistoryByStatus(final RepairHistoryData repairHistoryData)
    {
        LOG.info("Fetching repair history for status: {}", repairHistoryData.getStatus());
        ResultSet resultSet = getRepairHistoryInfo(repairHistoryData);
        List<Row> rows = StreamSupport.stream(resultSet.spliterator(), false).collect(Collectors.toList());

        List<RepairHistoryData> filteredHistoryData = rows.stream()
                .filter(row ->
                {
                    String status = row.getString(COLUMN_STATUS);
                    boolean matches = repairHistoryData.getStatus().name().equalsIgnoreCase(status);

                    LOG.debug("Row status: {}, matches: {}", status, matches);
                    return matches;
                })
                .map(row -> convertRowToRepairHistoryData(row, repairHistoryData.getLookBackTimeInMilliseconds()))
                .collect(Collectors.toList());

        return filteredHistoryData;
    }

    /**
     * Retrieves the repair history entries that fall within the look back time period.
     *
     * This method calculates the look back time based on the system's current time and the
     * look back duration specified in the ecc.yml configuration file. It then queries the
     * repair history records that have a start time greater than or equal to the look back time.
     *
     * @param repairHistoryData The data containing the look back time to filter the repair history.
     * @return A list of filtered RepairHistoryData objects that fall within the look back time period.
     */
    public List<RepairHistoryData> getRepairHistoryByLookBackTime(final RepairHistoryData repairHistoryData)
    {
        long currentTimeInMillis = System.currentTimeMillis();
        long lookBackTimeInMillis = repairHistoryData.getLookBackTimeInMilliseconds();
        long lookBackStartTime = currentTimeInMillis - lookBackTimeInMillis;

        LOG.info("Fetching repair history with look back start time: {}", lookBackStartTime);

        ResultSet resultSet = getRepairHistoryInfo(repairHistoryData);
        List<Row> rows = StreamSupport.stream(resultSet.spliterator(), false).collect(Collectors.toList());

        // Filter rows based on the look back time
        List<RepairHistoryData> filteredHistoryData = rows.stream()
                .filter(row ->
                {
                    Instant startedAt = row.get(COLUMN_STARTED_AT, Instant.class);
                    Instant finishedAt = row.get(COLUMN_FINISHED_AT, Instant.class);

                    long startedAtInMillis = startedAt.toEpochMilli();
                    long finishedAtInMillis = finishedAt.toEpochMilli();

                    // Ensure either startedAt or finishedAt falls within the lookback period
                    boolean withinLookBackPeriod = (startedAtInMillis >= lookBackStartTime || finishedAtInMillis >= lookBackStartTime);

                    LOG.debug("Row startedAt: {}, finishedAt: {}, within look back period: {}", startedAt, finishedAt,
                            withinLookBackPeriod);
                    return withinLookBackPeriod;
                })
                .map(row -> convertRowToRepairHistoryData(row, lookBackTimeInMillis))
                .collect(Collectors.toList());

        return filteredHistoryData;
    }

    public ResultSet getRepairHistoryInfo(final RepairHistoryData repairHistoryData)
    {
        BoundStatement boundStatement = mySelectStatement.bind(repairHistoryData.getTableId(),
                repairHistoryData.getNodeId());
        return myCqlSession.execute(boundStatement);
    }

    public ResultSet insertRepairHistoryInfo(final RepairHistoryData repairHistoryData)
    {
        LOG.info("Preparing to insert repair history with tableId {} and nodeId {}",
                repairHistoryData.getTableId(), repairHistoryData.getNodeId());
        BoundStatement repairHistoryInfo = myCreateStatement.bind(repairHistoryData.getTableId(),
                repairHistoryData.getNodeId(),
                repairHistoryData.getRepairId(),
                repairHistoryData.getJobId(),
                repairHistoryData.getCoordinatorId(),
                repairHistoryData.getRangeBegin(),
                repairHistoryData.getRangeEnd(),
                repairHistoryData.getParticipants(),
                repairHistoryData.getStatus().name(),
                repairHistoryData.getStartedAt(),
                repairHistoryData.getFinishedAt());
        ResultSet tmpResultSet = myCqlSession.execute(repairHistoryInfo);
        if (tmpResultSet.wasApplied())
        {
            LOG.info("RepairHistory inserted successfully with tableId {} and nodeId {}",
                    repairHistoryData.getTableId(), repairHistoryData.getNodeId());
        }
        else
        {
            LOG.error("Unable to insert repairHistory with tableId {} and nodeId {}",
                    repairHistoryData.getTableId(),
                    repairHistoryData.getNodeId());
        }
        return tmpResultSet;
    }

    @Override
    public Iterator<RepairEntry> iterate(final Node node,
            final TableReference tableReference,
            final long to,
            final Predicate<RepairEntry> predicate)
    {
        long from = System.currentTimeMillis() - myLookbackTimeInMs;
        return iterate(node, tableReference, to, from, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(
            final Node node,
            final TableReference tableReference,
            final long to,
            final long from,
            final Predicate<RepairEntry> predicate
    )
    {
        UUID start = Uuids.startOf(from);
        UUID finish = Uuids.endOf(to);

        Statement statement = myIterateStatement.bind(tableReference.getId(), node.getHostId(), start, finish);
        ResultSet resultSet = myCqlSession.execute(statement);

        return new RepairEntryIterator(node, tableReference, resultSet, predicate);
    }

    @Override
    public RepairSession newSession(
            final Node node,
            final TableReference tableReference,
            final UUID jobId,
            final LongTokenRange range,
            final Set<DriverNode> participants)
    {
        DriverNode driverNode = myNodeResolver.fromUUID(node.getHostId()).orElseThrow(IllegalStateException::new);
        Preconditions.checkArgument(participants.contains(driverNode),
                "Current node must be part of repair");

        return new RepairSessionImpl(tableReference.getId(), driverNode.getId(), jobId, range, participants);
    }

    public ResultSet updateRepairHistoryInfo(final RepairHistoryData repairHistoryData)
    {
        BoundStatement updateRepairHistoryInfo = myUpdateStatement.bind(repairHistoryData.getJobId(),
                repairHistoryData.getCoordinatorId(),
                repairHistoryData.getRangeBegin(),
                repairHistoryData.getRangeEnd(),
                repairHistoryData.getParticipants(),
                repairHistoryData.getStatus().name(),
                repairHistoryData.getStartedAt(),
                repairHistoryData.getFinishedAt(),
                repairHistoryData.getTableId(),
                repairHistoryData.getNodeId(),
                repairHistoryData.getRepairId());
        ResultSet tmpResultSet = myCqlSession.execute(updateRepairHistoryInfo);
        if (tmpResultSet.wasApplied())
        {
            LOG.info("RepairHistory successfully updated for tableId {} and nodeId {}",
                    repairHistoryData.getTableId(), repairHistoryData.getNodeId());
        }
        else
        {
            LOG.error("RepairHistory updated failed for tableId {} and nodeId {}",
                    repairHistoryData.getTableId(), repairHistoryData.getNodeId());
        }
        return tmpResultSet;
    }

    private RepairHistoryData convertRowToRepairHistoryData(final Row row, final long lookBackTimeInMs)
    {
        UUID tableId = row.get(COLUMN_TABLE_ID, UUID.class);
        UUID nodeId = row.get(COLUMN_NODE_ID, UUID.class);
        UUID repairId = row.get(COLUMN_REPAIR_ID, UUID.class);
        UUID jobId = row.get(COLUMN_JOB_ID, UUID.class);
        UUID coordinatorId = row.get(COLUMN_COORDINATOR_ID, UUID.class);
        String rangeBegin = row.get(COLUMN_RANGE_BEGIN, String.class);
        String rangeEnd = row.get(COLUMN_RANGE_END, String.class);
        Set<UUID> participants = row.getSet(COLUMN_PARTICIPANTS, UUID.class);
        String status = row.get(COLUMN_STATUS, String.class);
        Instant startedAt = row.get(COLUMN_STARTED_AT, Instant.class);
        Instant finishedAt = row.get(COLUMN_FINISHED_AT, Instant.class);
        RepairStatus repairStatus = RepairStatus.getFromStatus(status);

        return new RepairHistoryData.Builder()
                .withTableId(tableId)
                .withNodeId(nodeId)
                .withRepairId(repairId)
                .withJobId(jobId)
                .withCoordinatorId(coordinatorId)
                .withRangeBegin(rangeBegin)
                .withRangeEnd(rangeEnd)
                .withParticipants(participants)
                .withStatus(repairStatus)
                .withStartedAt(startedAt)
                .withFinishedAt(finishedAt)
                .withLookBackTimeInMilliseconds(lookBackTimeInMs)
                .build();
    }

    public final class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final TableReference tableReference;
        private final Iterator<Row> rowIterator;
        private final Predicate<RepairEntry> predicate;
        private final Node myNode;

        RepairEntryIterator(
                final Node node,
                final TableReference aTableReference,
                final ResultSet aResultSet,
                final Predicate<RepairEntry> aPredicate)
        {
            myNode = node;
            this.tableReference = aTableReference;
            this.rowIterator = aResultSet.iterator();
            this.predicate = aPredicate;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while (rowIterator.hasNext())
            {
                Row row = rowIterator.next();

                if (validateFields(row))
                {
                    RepairEntry repairEntry = buildFrom(row);
                    if (repairEntry != null && predicate.apply(repairEntry))
                    {
                        return repairEntry;
                    }
                }
            }

            return endOfData();
        }

        private RepairEntry buildFrom(final Row row)
        {
            long rangeBegin = Long.parseLong(row.getString(COLUMN_RANGE_BEGIN));
            long rangeEnd = Long.parseLong(row.getString(COLUMN_RANGE_END));

            LongTokenRange tokenRange = new LongTokenRange(rangeBegin, rangeEnd);
            long startedAt = row.getInstant(COLUMN_STARTED_AT).toEpochMilli();
            Instant finished = row.getInstant(COLUMN_FINISHED_AT);
            long finishedAt = -1L;
            if (finished != null)
            {
                finishedAt = finished.toEpochMilli();
            }
            Set<DriverNode> nodes = myReplicationState.getNodesClusterWide(tableReference, tokenRange, myNode);

            if (nodes == null)
            {
                LOG.debug("Token range {} was not found in metadata", tokenRange);
                return null;
            }
            String status = row.getString(COLUMN_STATUS);

            return new RepairEntry(tokenRange, startedAt, finishedAt, nodes, status);
        }

        private boolean validateFields(final Row row)
        {
            return !row.isNull(COLUMN_RANGE_BEGIN)
                    && !row.isNull(COLUMN_RANGE_END)
                    && !row.isNull(COLUMN_STARTED_AT)
                    && !row.isNull(COLUMN_STATUS);
        }
    }

    class RepairSessionImpl implements RepairSession
    {
        private final UUID myTableID;
        private final UUID myNodeID;
        private final UUID myJobID;
        private final LongTokenRange myRange;
        private final Set<UUID> myParticipants;
        private final AtomicReference<SessionState> mySessionState = new AtomicReference<>(SessionState.NO_STATE);
        private final AtomicReference<UUID> myRepairID = new AtomicReference<>(null);
        private final AtomicReference<Instant> myStartedAt = new AtomicReference<>(null);

        RepairSessionImpl(final UUID tableID,
                final UUID nodeID,
                final UUID jobID,
                final LongTokenRange range,
                final Set<DriverNode> participants)
        {
            myTableID = tableID;
            myNodeID = nodeID;
            myJobID = jobID;
            myRange = range;
            myParticipants = participants.stream()
                    .map(DriverNode::getId)
                    .collect(Collectors.toSet());
        }

        @VisibleForTesting
        UUID getId()
        {
            return myRepairID.get();
        }

        @Override
        public void start()
        {
            transitionTo(SessionState.STARTED);
            myStartedAt.compareAndSet(null, Instant.now());
        }

        /**
         * Transition to state DONE, as long as the previous status was STARTED. Set finished at to current timestamp.
         *
         * @param repairStatus The repair status
         */
        @Override
        public void finish(final RepairStatus repairStatus)
        {
            Preconditions.checkArgument(!RepairStatus.STARTED.equals(repairStatus),
                    "Repair status must change from started");
            transitionTo(SessionState.DONE);
            String rangeBegin = Long.toString(myRange.start);
            String rangeEnd = Long.toString(myRange.end);
            Instant finishedAt = Instant.now();
            myRepairID.compareAndSet(null, Uuids.timeBased());
            insertWithRetry(participant -> insertFinish(rangeBegin, rangeEnd, repairStatus, finishedAt, participant));
        }

        private void insertWithRetry(final Function<UUID, CompletionStage<AsyncResultSet>> insertFunction)
        {
            Map<UUID, CompletableFuture> futures = new HashMap<>();

            for (UUID participant : myParticipants)
            {
                CompletableFuture future = insertFunction.apply(participant).toCompletableFuture();
                futures.put(participant, future);
            }


            boolean loggedException = false;

            for (Map.Entry<UUID, CompletableFuture> entry : futures.entrySet())
            {
                CompletableFuture future = entry.getValue();

                try
                {
                    future.get(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException | TimeoutException e)
                {
                    UUID participant = entry.getKey();
                    LOG.warn("Unable to update repair history for {} - {}, retrying", participant, this);
                    if (!loggedException)
                    {
                        LOG.warn("", e);
                        loggedException = true;
                    }
                    insertFunction.apply(participant);
                }
            }
        }

        private CompletionStage<AsyncResultSet> insertFinish(final String rangeBegin,
                final String rangeEnd,
                final RepairStatus repairStatus,
                final Instant finishedAt,
                final UUID participant)
        {
            BoundStatement statement = myCreateStatement.bind(
                    myTableID,
                    myNodeID,
                    myRepairID.get(),
                    myJobID,
                    participant,
                    rangeBegin,
                    rangeEnd,
                    null,
                    repairStatus.toString(),
                    myStartedAt.get(),
                    finishedAt);

            return myCqlSession.executeAsync(statement);
        }

        /**
         * Return a string representation.
         *
         * @return String
         */
        @Override
        public String toString()
        {
            return String.format("table_id=%s,repair_id=%s,job_id=%s,range=%s,participants=%s", myTableID, myRepairID.get(),
                    myJobID, myRange, myParticipants);
        }

        private void transitionTo(final SessionState newState)
        {
            SessionState currentState = mySessionState.get();
            Preconditions.checkState(currentState.canTransition(newState),
                    "Cannot transition from " + currentState + " to " + newState);

            if (!mySessionState.compareAndSet(currentState, newState))
            {
                throw new IllegalStateException("Cannot transition from " + mySessionState.get() + " to " + newState);
            }
        }
    }
}
