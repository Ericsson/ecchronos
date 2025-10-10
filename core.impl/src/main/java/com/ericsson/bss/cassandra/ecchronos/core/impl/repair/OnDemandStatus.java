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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

public final class OnDemandStatus
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandStatus.class);

    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "on_demand_repair_status";
    private static final String HOST_ID_COLUMN_NAME = "host_id";
    private static final String STATUS_COLUMN_NAME = "status";
    private static final String JOB_ID_COLUMN_NAME = "job_id";
    private static final String REPAIR_TYPE_COLUMN_NAME = "repair_type";
    private static final String TABLE_REFERENCE_COLUMN_NAME = "table_reference";
    private static final String TOKEN_MAP_HASH_COLUMN_NAME = "token_map_hash";
    private static final String REPAIRED_TOKENS_COLUMN_NAME = "repaired_tokens";
    private static final String UDT_TOKEN_RANGE_NAME = "token_range";
    private static final String UDT_START_TOKEN_NAME = "start";
    private static final String UDT_END_TOKEN_NAME = "end";
    private static final String UDT_TABLE_REFERENCE_NAME = "table_reference";
    private static final String UDT_ID_NAME = "id";
    private static final String UDT_KEYSPACE_NAME = "keyspace_name";
    private static final String UDT_TABLE_NAME = "table_name";
    private static final String COMPLETED_TIME_COLUMN_NAME = "completed_time";

    private final CqlSession mySession;
    private final UserDefinedType myUDTTokenType;
    private final UserDefinedType myUDTTableReferenceType;
    private final PreparedStatement myGetStatusStatement;
    private final PreparedStatement myInsertNewJobStatement;
    private final PreparedStatement myUpdateRepairedTokenForJobStatement;
    private final PreparedStatement myUpdateJobToFinishedStatement;
    private final PreparedStatement myUpdateJobToFailedStatement;
    private final TableReferenceFactory myTableReferenceFactory;

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    /**
     * Constructor.
     *
     * @param nativeConnectionProvider The native connection provider.
     */
    public OnDemandStatus(final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        mySession = nativeConnectionProvider.getCqlSession();
        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession);
        myUDTTokenType = mySession.getMetadata()
                .getKeyspace(KEYSPACE_NAME)
                .flatMap(ks -> ks.getUserDefinedType(UDT_TOKEN_RANGE_NAME))
                .orElseThrow(() -> new IllegalArgumentException("Missing UDT " + UDT_TOKEN_RANGE_NAME));
        myUDTTableReferenceType = mySession.getMetadata()
                .getKeyspace(KEYSPACE_NAME)
                .flatMap(ks -> ks.getUserDefinedType(UDT_TABLE_REFERENCE_NAME))
                .orElseThrow(() -> new IllegalArgumentException("Missing UDT " + UDT_TABLE_REFERENCE_NAME));

        SimpleStatement getStatusStatement = selectFrom(KEYSPACE_NAME, TABLE_NAME)
                .all()
                .whereColumn(HOST_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement insertNewJobStatement = insertInto(KEYSPACE_NAME, TABLE_NAME)
                .value(HOST_ID_COLUMN_NAME, bindMarker())
                .value(JOB_ID_COLUMN_NAME, bindMarker())
                .value(TABLE_REFERENCE_COLUMN_NAME, bindMarker())
                .value(TOKEN_MAP_HASH_COLUMN_NAME, bindMarker())
                .value(REPAIRED_TOKENS_COLUMN_NAME, bindMarker())
                .value(REPAIR_TYPE_COLUMN_NAME, bindMarker())
                .value(STATUS_COLUMN_NAME, literal("started"))
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement updateRepairedTokenForJobStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(REPAIRED_TOKENS_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement updateJobToFinishedStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(STATUS_COLUMN_NAME, literal("finished"))
                .setColumn(COMPLETED_TIME_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement updateJobToFailedStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(STATUS_COLUMN_NAME, literal("failed"))
                .setColumn(COMPLETED_TIME_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        myGetStatusStatement = mySession.prepare(getStatusStatement);
        myInsertNewJobStatement = mySession.prepare(insertNewJobStatement);
        myUpdateRepairedTokenForJobStatement = mySession.prepare(updateRepairedTokenForJobStatement);
        myUpdateJobToFinishedStatement = mySession.prepare(updateJobToFinishedStatement);
        myUpdateJobToFailedStatement = mySession.prepare(updateJobToFailedStatement);
    }

    public Map<UUID, Node> getNodes()
    {
        return myNativeConnectionProvider.getNodes();
    }

    /**
     * Retrieves the ongoing repair jobs with a 'started' status for all nodes managed by this instance.
     *
     * <p>This method iterates over the list of local nodes and fetches ongoing repair jobs for each node.
     * It uses the {@link #processResultSet(ReplicationState, UUID)} method to process the result set
     * for each node, filtering only those jobs whose status is 'started'. The results are aggregated
     * into a map, where the key is the host ID of the node, and the value is a set of ongoing jobs
     * associated with that node.</p>
     *
     * @param replicationState The replication state used to determine the repair status for each node.
     *                         This parameter is required to evaluate the ongoing jobs accurately.
     * @return A map where each key represents a node's host ID and each value is a set of ongoing
     *         repair jobs for that node. Only includes nodes managed by this ecChronos instance.
     */
    public Map<UUID, Set<OngoingJob>> getOngoingStartedJobsForAllNodes(final ReplicationState replicationState)
    {
        Map<UUID, Set<OngoingJob>> allOngoingJobs = new HashMap<>();
        for (Node node : getNodes().values())
        {
            Set<OngoingJob> ongoingJobs = processResultSet(replicationState, node.getHostId());
            allOngoingJobs.put(node.getHostId(), ongoingJobs);
        }
        return allOngoingJobs;
    }

    /**
     * Get all jobs for this host that have the status 'started'.
     *
     * @param replicationState The replication state.
     * @return Set of ongoing jobs
     */
    public Set<OngoingJob> getOngoingJobs(final ReplicationState replicationState, final UUID hostId)
    {
        return processResultSet(replicationState, hostId);
    }

    private Set<OngoingJob> processResultSet(final ReplicationState replicationState, final UUID hostId)
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(hostId));

        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Row row : result.all())
        {
            OngoingJob.Status status;
            try
            {
                String statusStr = row.getString(STATUS_COLUMN_NAME);
                if (statusStr == null || statusStr.trim().isEmpty())
                {
                    LOG.warn("Ignoring table repair job with id {}, status is null or empty",
                            row.getUuid(JOB_ID_COLUMN_NAME));
                    continue;
                }
                status = OngoingJob.Status.valueOf(statusStr);
            }
            catch (IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {}, unable to parse status: {}",
                        row.getUuid(JOB_ID_COLUMN_NAME),
                        row.getString(STATUS_COLUMN_NAME));
                continue;
            }

            if (status.equals(OngoingJob.Status.started))
            {
                createOngoingJob(replicationState, ongoingJobs, row, status, hostId);
            }
        }
        return ongoingJobs;
    }

    /**
     * Get jobs for all the nodes, independent of their status.
     *
     * @return  Set of ongoing jobs
     */
    public Set<OngoingJob> getAllClusterWideJobs()
    {
        NodeResolver nodeResolver = new NodeResolverImpl(mySession);
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Node node : getNodes().values())
        {
            ReplicationState replState = new ReplicationStateImpl(nodeResolver, mySession);
            ongoingJobs.addAll(getAllJobsForHost(replState, node.getHostId()));
        }
        return ongoingJobs;
    }

    /**
     * Get all jobs for this host, independent of the status.
     *
     * @param replicationState The replication state
     * @param hostId
     * @return Set of ongoing jobs
     */
    public Set<OngoingJob> getAllJobs(final ReplicationState replicationState, final UUID hostId)
    {
        return getAllJobsForHost(replicationState, hostId);
    }

    private Set<OngoingJob> getAllJobsForHost(final ReplicationState replicationState, final UUID hostId)
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(hostId));

        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Row row : result.all())
        {
            OngoingJob.Status status;
            try
            {
                String statusStr = row.getString(STATUS_COLUMN_NAME);
                if (statusStr == null || statusStr.trim().isEmpty())
                {
                    LOG.warn("Ignoring table repair job with id {} and hostId {}, status is null or empty",
                            row.getUuid(JOB_ID_COLUMN_NAME),
                            hostId);
                    continue;
                }
                status = OngoingJob.Status.valueOf(statusStr);
            }
            catch (IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {} and hostId {}, unable to parse status: {}",
                        row.getUuid(JOB_ID_COLUMN_NAME),
                        hostId,
                        row.getString(STATUS_COLUMN_NAME));
                continue;
            }

            createOngoingJob(replicationState, ongoingJobs, row, status, hostId);
        }

        return ongoingJobs;
    }

    private void createOngoingJob(final ReplicationState replicationState,
                                  final Set<OngoingJob> ongoingJobs,
                                  final Row row,
                                  final OngoingJob.Status status,
                                  final UUID hostId)
    {
        // Only process jobs for nodes that exist in our node list (prevents OngoingJob constructor failure)
        boolean nodeExistsInList = getNodes().containsKey(hostId);
        if (!nodeExistsInList)
        {
            LOG.info("Skipping repair job for node {} as it's not in our node list", hostId);
            return;
        }

        UUID jobId = row.getUuid(JOB_ID_COLUMN_NAME);
        int tokenMapHash = row.getInt(TOKEN_MAP_HASH_COLUMN_NAME);
        Set<UdtValue> repairedTokens = row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UdtValue.class);
        UdtValue uDTTableReference = row.getUdtValue(TABLE_REFERENCE_COLUMN_NAME);
        String keyspace = uDTTableReference.getString(UDT_KEYSPACE_NAME);
        String table = uDTTableReference.getString(UDT_TABLE_NAME);
        TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
        Instant completed = row.get(COMPLETED_TIME_COLUMN_NAME, Instant.class);
        RepairType repairType = RepairType.VNODE;
        String repairTypeStr = row.getString(REPAIR_TYPE_COLUMN_NAME);
        if (repairTypeStr != null && !repairTypeStr.isEmpty())
        {
            repairType = RepairType.valueOf(repairTypeStr);
        }
        Long completedTime = null;
        if (completed != null)
        {
            completedTime = completed.toEpochMilli();
        }

        if (uDTTableReference.getUuid(UDT_ID_NAME).equals(tableReference.getId()))
        {
            OngoingJob ongoingJob = new OngoingJob.Builder()
                    .withOnDemandStatus(this)
                    .withTableReference(tableReference)
                    .withReplicationState(replicationState)
                    .withOngoingJobInfo(jobId, tokenMapHash, repairedTokens, status, completedTime, repairType)
                    .withHostId(hostId)
                    .withRepairType(repairType)
                    .build();
            ongoingJobs.add(ongoingJob);
        }
        else
        {
            LOG.info("Ignoring table repair job with id {} of table {} as it was for table {}.{}({})",
                    jobId,
                    tableReference,
                    keyspace,
                    table,
                    uDTTableReference.getUuid(UDT_ID_NAME));
        }
    }

    /**
     * Add a new job.
     *
     * @param jobId The job id.
     * @param tableReference The table reference.
     * @param tokenMapHash The token map hash.
     */
    public void addNewJob(final UUID hostId,
                          final UUID jobId,
                          final TableReference tableReference,
                          final int tokenMapHash,
                          final RepairType repairType)
    {
        addNewJob(hostId, jobId, tableReference, tokenMapHash, Collections.EMPTY_SET, repairType);
    }

    /**
     * Add a new job for a specific host.
     *
     * @param jobId The job id.
     * @param host The host.
     * @param tableReference The table reference.
     * @param tokenMapHash The token map hash.
     * @param repairedRanges The ranges.
     */
    public void addNewJob(final UUID host,
                          final UUID jobId,
                          final TableReference tableReference,
                          final int tokenMapHash,
                          final Set<LongTokenRange> repairedRanges,
                          final RepairType repairType)
    {
        Set<UdtValue> repairedRangesUDT = new HashSet<>();
        if (repairedRanges != null)
        {
            repairedRanges.forEach(t -> repairedRangesUDT.add(createUDTTokenRangeValue(t.start, t.end)));
        }
        UdtValue uDTTableReference = myUDTTableReferenceType.newValue()
                .setUuid(UDT_ID_NAME, tableReference.getId())
                .setString(UDT_KEYSPACE_NAME, tableReference.getKeyspace())
                .setString(UDT_TABLE_NAME, tableReference.getTable());
        BoundStatement statement = myInsertNewJobStatement.bind(host, jobId, uDTTableReference, tokenMapHash,
                repairedRangesUDT, repairType.toString());
        mySession.execute(statement);
    }

    /**
     * Update job with repaired tokens.
     *
     * @param hostId
     * @param jobId Job id.
     * @param repairedTokens Repaired tokens.
     */
    public void updateJob(final UUID hostId, final UUID jobId, final Set<UdtValue> repairedTokens)
    {
        mySession.execute(myUpdateRepairedTokenForJobStatement.bind(repairedTokens, hostId, jobId));
    }

    /**
     * Update a job as finished with current time stamp.
     *
     * @param jobId Id of the job set as finished.
     * @param hostId
     */
    public void finishJob(final UUID jobId, final UUID hostId)
    {
        mySession.execute(myUpdateJobToFinishedStatement.bind(Instant.ofEpochMilli(System.currentTimeMillis()),
                hostId, jobId));
    }

    /**
     * Update a job to failed status with current timestamp.
     *
     * @param jobId Id of the job to set as failed.
     * @param hostId
     */
    public void failJob(final UUID jobId, final UUID hostId)
    {
        mySession.execute(myUpdateJobToFailedStatement.bind(Instant.ofEpochMilli(System.currentTimeMillis()),
                hostId, jobId));
    }

    /**
     * Create a new UDT token range value from the given start and end tokens.
     *
     * @param start Start token.
     * @param end End token.
     * @return UdtValue
     */
    public UdtValue createUDTTokenRangeValue(final Long start, final Long end)
    {
        return myUDTTokenType.newValue()
                .setString(UDT_START_TOKEN_NAME, start.toString())
                .setString(UDT_END_TOKEN_NAME, end.toString());
    }

    /**
     * Get the start token from a UDT.
     *
     * @param t The UDT value.
     * @return long
     */
    public long getStartTokenFrom(final UdtValue t)
    {
        return Long.valueOf(t.getString(UDT_START_TOKEN_NAME));
    }

    /**
     * Get the end token from a UDT.
     *
     * @param t The UDT value.
     * @return long
     */
    public long getEndTokenFrom(final UdtValue t)
    {
        return Long.valueOf(t.getString(UDT_END_TOKEN_NAME));
    }
}
