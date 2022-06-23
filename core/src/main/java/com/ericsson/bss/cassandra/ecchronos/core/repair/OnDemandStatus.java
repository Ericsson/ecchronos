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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

public class OnDemandStatus
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandStatus.class);

    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "on_demand_repair_status";
    private static final String HOST_ID_COLUMN_NAME = "host_id";
    private static final String STATUS_COLUMN_NAME = "status";
    private static final String JOB_ID_COLUMN_NAME = "job_id";
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
    private final UUID myHostId;
    private final UserDefinedType myUDTTokenType;
    private final UserDefinedType myUDTTableReferenceType;
    private final PreparedStatement myGetStatusStatement;
    private final PreparedStatement myInsertNewJobStatement;
    private final PreparedStatement myUpdateRepairedTokenForJobStatement;
    private final PreparedStatement myUpdateJobToFinishedStatement;
    private final PreparedStatement myUpdateJobToFailedStatement;
    private final TableReferenceFactory myTableReferenceFactory;

    public OnDemandStatus(NativeConnectionProvider nativeConnectionProvider)
    {
        mySession = nativeConnectionProvider.getSession();
        myHostId = nativeConnectionProvider.getLocalNode().getHostId();
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
                .whereColumn(HOST_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement insertNewJobStatement = insertInto(KEYSPACE_NAME, TABLE_NAME)
                .value(HOST_ID_COLUMN_NAME, bindMarker())
                .value(JOB_ID_COLUMN_NAME, bindMarker())
                .value(TABLE_REFERENCE_COLUMN_NAME, bindMarker())
                .value(TOKEN_MAP_HASH_COLUMN_NAME, bindMarker())
                .value(REPAIRED_TOKENS_COLUMN_NAME, bindMarker())
                .value(STATUS_COLUMN_NAME, bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement updateRepairedTokenForJobStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(REPAIRED_TOKENS_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        //TODO 2 BELOW ARE EXACTLY THE SAME
        SimpleStatement updateJobToFinishedStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(STATUS_COLUMN_NAME, bindMarker())
                .setColumn(COMPLETED_TIME_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        SimpleStatement updateJobToFailedStatement = update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(STATUS_COLUMN_NAME, bindMarker())
                .setColumn(COMPLETED_TIME_COLUMN_NAME, bindMarker())
                .whereColumn(HOST_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .whereColumn(JOB_ID_COLUMN_NAME).isEqualTo(bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        myGetStatusStatement = mySession.prepare(getStatusStatement);
        myInsertNewJobStatement = mySession.prepare(insertNewJobStatement);
        myUpdateRepairedTokenForJobStatement = mySession.prepare(updateRepairedTokenForJobStatement);
        myUpdateJobToFinishedStatement = mySession.prepare(updateJobToFinishedStatement);
        myUpdateJobToFailedStatement = mySession.prepare(updateJobToFailedStatement);
    }

    public UUID getHostId()
    {
        return myHostId;
    }

    public Set<OngoingJob> getOngoingJobs(ReplicationState replicationState)
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(myHostId));

        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Row row : result.all())
        {
            Status status;
            try
            {
                status = Status.valueOf(row.getString(STATUS_COLUMN_NAME));
            }
            catch (IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {}, unable to parse status",
                        row.getUuid(JOB_ID_COLUMN_NAME));
                continue;
            }

            if (status.equals(Status.started))
            {
                createOngoingJob(replicationState, ongoingJobs, row, status, myHostId);
            }
        }

        return ongoingJobs;
    }

    public Set<OngoingJob> getAllClusterWideJobs()
    {
        Metadata metadata = mySession.getMetadata();
        NodeResolver nodeResolver = new NodeResolverImpl(mySession);
        Collection<Node> nodes = metadata.getNodes().values();
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Node node : nodes)
        {
            ReplicationState replState = new ReplicationStateImpl(nodeResolver, mySession, node);
            ongoingJobs.addAll(getAllJobsForHost(replState, node.getHostId()));
        }
        return ongoingJobs;
    }

    public Set<OngoingJob> getAllJobs(ReplicationState replicationState)
    {
        return getAllJobsForHost(replicationState, myHostId);
    }

    private Set<OngoingJob> getAllJobsForHost(ReplicationState replicationState, UUID hostId)
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(hostId));

        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for (Row row : result.all())
        {
            Status status;
            try
            {
                status = Status.valueOf(row.getString(STATUS_COLUMN_NAME));
            }
            catch (IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {} and hostId {}, unable to parse status",
                        row.getUuid(JOB_ID_COLUMN_NAME), hostId);
                continue;
            }

            createOngoingJob(replicationState, ongoingJobs, row, status, hostId);
        }

        return ongoingJobs;
    }

    private void createOngoingJob(ReplicationState replicationState, Set<OngoingJob> ongoingJobs, Row row,
            Status status, UUID hostId)
    {
        UUID jobId = row.getUuid(JOB_ID_COLUMN_NAME);
        int tokenMapHash = row.getInt(TOKEN_MAP_HASH_COLUMN_NAME);
        Set<UdtValue> repairedTokens = row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UdtValue.class);
        UdtValue uDTTableReference = row.getUdtValue(TABLE_REFERENCE_COLUMN_NAME);
        String keyspace = uDTTableReference.getString(UDT_KEYSPACE_NAME);
        String table = uDTTableReference.getString(UDT_TABLE_NAME);
        TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
        Instant completed = row.get(COMPLETED_TIME_COLUMN_NAME, Instant.class);
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
                    .withOngoingJobInfo(jobId, tokenMapHash, repairedTokens, status, completedTime)
                    .withHostId(hostId)
                    .build();
            ongoingJobs.add(ongoingJob);
        }
        else
        {
            LOG.info("Ignoring table repair job with id {} of table {} as it was for table {}.{}({})", jobId,
                    tableReference, keyspace, table, uDTTableReference.getUuid(UDT_ID_NAME));
        }
    }

    public void addNewJob(UUID jobId, TableReference tableReference, int tokenMapHash)
    {
        addNewJob(myHostId, jobId, tableReference, tokenMapHash, Collections.EMPTY_SET);
    }

    public void addNewJob(UUID host, UUID jobId, TableReference tableReference, int tokenMapHash,
            Set<LongTokenRange> repairedRanges)
    {
        Set<UdtValue> repairedRangesUDT = new HashSet<>();
        if (repairedRanges != null)
        {
            repairedRanges.forEach(t -> repairedRangesUDT.add(createUDTTokenRangeValue(t.start, t.end)));
        }
        UdtValue uDTTableReference = myUDTTableReferenceType.newValue().setUuid(UDT_ID_NAME, tableReference.getId())
                .setString(UDT_KEYSPACE_NAME, tableReference.getKeyspace())
                .setString(UDT_TABLE_NAME, tableReference.getTable());
        BoundStatement statement = myInsertNewJobStatement.bind(host, jobId, uDTTableReference, tokenMapHash,
                repairedRangesUDT, "started");
        mySession.execute(statement);
    }

    public void updateJob(UUID jobId, Set<UdtValue> repairedTokens)
    {
        mySession.execute(myUpdateRepairedTokenForJobStatement.bind(repairedTokens, myHostId, jobId));
    }

    public void finishJob(UUID jobId)
    {
        mySession.execute(myUpdateJobToFinishedStatement.bind("finished", Instant.ofEpochMilli(System.currentTimeMillis()), myHostId, jobId));
    }

    public void failJob(UUID jobId)
    {
        mySession.execute(myUpdateJobToFailedStatement.bind("failed", Instant.ofEpochMilli(System.currentTimeMillis()), myHostId, jobId));
    }

    public UdtValue createUDTTokenRangeValue(Long start, Long end)
    {
        return myUDTTokenType.newValue().setString(UDT_START_TOKEN_NAME, start.toString())
                .setString(UDT_END_TOKEN_NAME, end.toString());
    }

    public long getStartTokenFrom(UdtValue t)
    {
        return Long.valueOf(t.getString(UDT_START_TOKEN_NAME));
    }

    public long getEndTokenFrom(UdtValue t)
    {
        return Long.valueOf(t.getString(UDT_END_TOKEN_NAME));
    }
}
