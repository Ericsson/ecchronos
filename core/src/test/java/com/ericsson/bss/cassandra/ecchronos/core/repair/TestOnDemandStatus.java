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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.ericsson.bss.cassandra.ecchronos.core.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestOnDemandStatus extends AbstractCassandraTest
{
    private static final String STATUS_FAILED = "failed";
	private static final String STATUS_FINISHED = "finished";
	private static final String STATUS_STARTED = "started";
	private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "on_demand_repair_status";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final String HOST_ID_COLUMN_NAME = "host_id";
    private static final String STATUS_COLUMN_NAME = "status";
    private static final String JOB_ID_COLUMN_NAME = "job_id";
    private static final String TABLE_REFERENCE_COLUMN_NAME = "table_reference";
    private static final String TOKEN_MAP_HASH_COLUMN_NAME = "token_map_hash";
    private static final String REPAIRED_TOKENS_COLUMN_NAME = "repaired_tokens";
    private static final String UDT_TABLE_REFERENCE_NAME = "table_reference";
    private static final String UDT_ID_NAME = "id";
    private static final String UDT_KAYSPACE_NAME = "keyspace_name";
    private static final String UDT_TABLE_NAME = "table_name";
    private static final String COMPLEDED_TIME_COLUMN_NAME = "completed_time";

    private UUID myHostId;
    private TableReferenceFactory myTableReferenceFactory;
    private UserType myUDTTableReferenceType;

    @Mock
    private ReplicationState myReplicationState;

	@Before
    public void startup()
    {
		myHostId = getNativeConnectionProvider().getLocalHost().getHostId();

        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", KEYSPACE_NAME ));
        mySession.execute(String.format("CREATE TYPE IF NOT EXISTS %s.token_range (start text, end text)", KEYSPACE_NAME));
        mySession.execute(String.format("CREATE TYPE IF NOT EXISTS %s.table_reference (id uuid, keyspace_name text, table_name text)", KEYSPACE_NAME));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (host_id uuid, job_id uuid, table_reference frozen<table_reference>, token_map_hash int, repaired_tokens frozen<set<frozen<token_range>>>, status text, completed_time timestamp, PRIMARY KEY(host_id, job_id)) WITH default_time_to_live = 2592000 AND gc_grace_seconds = 0", KEYSPACE_NAME, TABLE_NAME));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (col1 int, col2 int, PRIMARY KEY(col1))", KEYSPACE_NAME, TEST_TABLE_NAME));

        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession.getCluster().getMetadata());
        myUDTTableReferenceType = mySession.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME).getUserType(UDT_TABLE_REFERENCE_NAME);
    }

    @After
    public void testCleanup()
    {
        mySession.execute(String.format("TRUNCATE %s.%s", KEYSPACE_NAME, TABLE_NAME));
    }

    @Test
    public void testOndemandStatusIsCreated()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        assertThat(onDemandStatus).isNotNull();
    }

    @Test
    public void testUDTToken()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        Long start = -10L;
        Long end = 100L;
        UDTValue token = onDemandStatus.createUDTTokenRangeValue(start, end);
        assertThat(onDemandStatus.getStartTokenFrom(token)).isEqualTo(start);
        assertThat(onDemandStatus.getEndTokenFrom(token)).isEqualTo(end);
    }

    @Test
    public void testAddNewJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        ResultSet result = mySession.execute("SELECT * FROM " + KEYSPACE_NAME + "." + TABLE_NAME);

        List<Row> rows = result.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);
        assertThat(row.getUUID(HOST_ID_COLUMN_NAME)).isEqualByComparingTo(myHostId);
        assertThat(row.getUUID(JOB_ID_COLUMN_NAME)).isEqualByComparingTo(jobId);
        UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
        assertThat(uDTTableReference).isNotNull();
        assertThat(uDTTableReference.getType()).isEqualTo(myUDTTableReferenceType);
        assertThat(uDTTableReference.getUUID(UDT_ID_NAME)).isEqualTo(tableReference.getId());
        assertThat(uDTTableReference.getString(UDT_KAYSPACE_NAME)).isEqualTo(tableReference.getKeyspace());
        assertThat(uDTTableReference.getString(UDT_TABLE_NAME)).isEqualTo(tableReference.getTable());
        assertThat(row.getInt(TOKEN_MAP_HASH_COLUMN_NAME)).isEqualTo(hashValue);
        assertThat(row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UDTValue.class)).isEmpty();
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo(STATUS_STARTED);
        assertThat(row.get(COMPLEDED_TIME_COLUMN_NAME, Long.class)).isNull();
    }

    @Test
    public void testUpdateRepariedTokkens()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        ResultSet result = mySession.execute("SELECT * FROM " + KEYSPACE_NAME + "." + TABLE_NAME);

        List<Row> rows = result.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);
        assertThat(row.getUUID(HOST_ID_COLUMN_NAME)).isEqualByComparingTo(myHostId);
        assertThat(row.getUUID(JOB_ID_COLUMN_NAME)).isEqualByComparingTo(jobId);
        UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
        assertThat(uDTTableReference).isNotNull();
        assertThat(uDTTableReference.getType()).isEqualTo(myUDTTableReferenceType);
        assertThat(uDTTableReference.getUUID(UDT_ID_NAME)).isEqualTo(tableReference.getId());
        assertThat(uDTTableReference.getString(UDT_KAYSPACE_NAME)).isEqualTo(tableReference.getKeyspace());
        assertThat(row.getInt(TOKEN_MAP_HASH_COLUMN_NAME)).isEqualTo(hashValue);
        assertThat(row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UDTValue.class)).isEqualTo(repairedTokens);
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo(STATUS_STARTED);
        assertThat(row.get(COMPLEDED_TIME_COLUMN_NAME, Long.class)).isNull();
    }

    @Test
    public void testUpdateToFinished()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        onDemandStatus.finishJob(jobId);

        ResultSet result = mySession.execute("SELECT * FROM " + KEYSPACE_NAME + "." + TABLE_NAME);

        List<Row> rows = result.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);
        assertThat(row.getUUID(HOST_ID_COLUMN_NAME)).isEqualByComparingTo(myHostId);
        assertThat(row.getUUID(JOB_ID_COLUMN_NAME)).isEqualByComparingTo(jobId);
        UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
        assertThat(uDTTableReference).isNotNull();
        assertThat(uDTTableReference.getType()).isEqualTo(myUDTTableReferenceType);
        assertThat(uDTTableReference.getUUID(UDT_ID_NAME)).isEqualTo(tableReference.getId());
        assertThat(uDTTableReference.getString(UDT_KAYSPACE_NAME)).isEqualTo(tableReference.getKeyspace());
        assertThat(row.getInt(TOKEN_MAP_HASH_COLUMN_NAME)).isEqualTo(hashValue);
        assertThat(row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UDTValue.class)).isEqualTo(repairedTokens);
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo(STATUS_FINISHED);
        assertThat(row.get(COMPLEDED_TIME_COLUMN_NAME, Long.class)).isNotNull();
    }

    @Test
    public void testUpdateToFailed()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        onDemandStatus.failJob(jobId);

        ResultSet result = mySession.execute("SELECT * FROM " + KEYSPACE_NAME + "." + TABLE_NAME);

        List<Row> rows = result.all();
        assertThat(rows.size()).isEqualTo(1);

        Row row = rows.get(0);
        assertThat(row.getUUID(HOST_ID_COLUMN_NAME)).isEqualByComparingTo(myHostId);
        assertThat(row.getUUID(JOB_ID_COLUMN_NAME)).isEqualByComparingTo(jobId);
        UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
        assertThat(uDTTableReference).isNotNull();
        assertThat(uDTTableReference.getType()).isEqualTo(myUDTTableReferenceType);
        assertThat(uDTTableReference.getUUID(UDT_ID_NAME)).isEqualTo(tableReference.getId());
        assertThat(uDTTableReference.getString(UDT_KAYSPACE_NAME)).isEqualTo(tableReference.getKeyspace());
        assertThat(row.getInt(TOKEN_MAP_HASH_COLUMN_NAME)).isEqualTo(hashValue);
        assertThat(row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UDTValue.class)).isEqualTo(repairedTokens);
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo(STATUS_FAILED);
        assertThat(row.get(COMPLEDED_TIME_COLUMN_NAME, Long.class)).isNotNull();
    }

    @Test
    public void testGetAllClusterWideJobsNoJobs()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        Set<OngoingJob> clusterWideJobs = onDemandStatus.getAllClusterWideJobs();

        assertThat(clusterWideJobs).isEmpty();
    }

    @Test
    public void testGetAllClusterWideJobs()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllClusterWideJobs();

        assertThat(ongoingJobs.size()).isEqualTo(1);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEmpty();
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1L);
    }

    @Test
    public void testGetOngoingJobsNoJobs()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetOngoingJobsWithNewJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        assertThat(ongoingJobs.size()).isEqualTo(1);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEmpty();
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1L);
    }

    @Test
    public void testGetOngoingJobsWithNewTable()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        mySession.execute("DROP TABLE " + KEYSPACE_NAME + "." + TEST_TABLE_NAME);
        mySession.execute(String.format("CREATE TABLE %s.%s (col1 int, col2 int, PRIMARY KEY(col1))", KEYSPACE_NAME, TEST_TABLE_NAME));

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetOngoingJobsWithUpdatedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1L);
    }

    @Test
    public void testGetOngoingJobsWithFinishedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);
        onDemandStatus.finishJob(jobId);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetOngoingJobsWithFailedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);
        onDemandStatus.failJob(jobId);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getOngoingJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetAllJobsNoJobs()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetAllJobsWithNewJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        assertThat(ongoingJobs.size()).isEqualTo(1);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEmpty();
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1L);
    }

    @Test
    public void testGetAllJobsWithNewTable()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        mySession.execute("DROP TABLE " + KEYSPACE_NAME + "." + TEST_TABLE_NAME);
        mySession.execute(String.format("CREATE TABLE %s.%s (col1 int, col2 int, PRIMARY KEY(col1))", KEYSPACE_NAME, TEST_TABLE_NAME));

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetAllJobsWithUpdatedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1L);
    }

    @Test
    public void testGetAllJobsWithFinishedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);
        onDemandStatus.finishJob(jobId);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.finished);
        assertThat(ongoingJob.getCompletedTime()).isPositive();
    }

    @Test
    public void testGetAllJobsWithFailedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TEST_TABLE_NAME);
        Map<LongTokenRange, ImmutableSet<Node>> tokenMap = new HashMap<>();
        when(myReplicationState.getTokenRangeToReplicas(tableReference)).thenReturn(tokenMap );
        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);
        onDemandStatus.failJob(jobId);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getAllJobs(myReplicationState);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TEST_TABLE_NAME);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.failed);
        assertThat(ongoingJob.getCompletedTime()).isPositive();
   }
}
