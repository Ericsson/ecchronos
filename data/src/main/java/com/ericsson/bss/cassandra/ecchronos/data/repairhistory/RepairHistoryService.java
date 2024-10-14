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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public final class RepairHistoryService
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
    private final CqlSession myCqlSession;

    public RepairHistoryService(final CqlSession cqlSession)
    {
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
}
