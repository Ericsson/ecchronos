package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.ericsson.bss.cassandra.ecchronos.core.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOnDemandStatus extends AbstractCassandraTest
{
    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "ondemand_status";
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

    private UUID myHostId;
    private TableReferenceFactory myTableReferenceFactory;
    private UserType myUDTTableReferenceType;

	@Before
    public void startup()
    {
		myHostId = getNativeConnectionProvider().getLocalHost().getHostId();

        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", KEYSPACE_NAME ));
        mySession.execute(String.format("CREATE TYPE IF NOT EXISTS %s.token_range (start text, end text)", KEYSPACE_NAME));
        mySession.execute(String.format("CREATE TYPE IF NOT EXISTS %s.table_reference (id uuid, keyspace_name text, table_name text)", KEYSPACE_NAME));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (host_id uuid, job_id uuid, table_reference frozen<table_reference>, token_map_hash int, repaired_tokens frozen<set<frozen<token_range>>>, status text, PRIMARY KEY(host_id, job_id)) WITH default_time_to_live = 2592000 AND gc_grace_seconds = 0", KEYSPACE_NAME, TABLE_NAME));

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
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

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
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo("started");
    }

    @Test
    public void testUpdateRepariedTokkens()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

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
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo("started");
    }

    @Test
    public void testUpdateToFinished()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

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
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo("finished");
    }

    @Test
    public void testUpdateToFailed()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

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
        assertThat(row.getString(STATUS_COLUMN_NAME)).isEqualTo("failed");
    }

    @Test
    public void testGetOngoingJobsNoJobs()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        Set<OngoingJob> ongoingJobs = onDemandStatus.getMyOngoingJobs();

        assertThat(ongoingJobs).isEmpty();
    }

    @Test
    public void testGetOngoingJobsWithNewJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getMyOngoingJobs();

        assertThat(ongoingJobs.size()).isEqualTo(1);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TABLE_NAME);
        assertThat(ongoingJob.getTokenMapHash()).isEqualTo(hashValue);
        assertThat(ongoingJob.getRepiaredTokens()).isEmpty();
    }

    @Test
    public void testGetOngoingJobsWithUpdatedJob()
    {
        OnDemandStatus onDemandStatus = new OnDemandStatus(getNativeConnectionProvider());

        UUID jobId = UUID.randomUUID();
        int hashValue = 1;
        TableReference tableReference = myTableReferenceFactory.forTable(KEYSPACE_NAME, TABLE_NAME);

        onDemandStatus.addNewJob(jobId, tableReference, hashValue);

        Set<UDTValue> repairedTokens = new HashSet<>();
        repairedTokens.add(onDemandStatus.createUDTTokenRangeValue(-50L, 700L));
        onDemandStatus.updateJob(jobId, repairedTokens);

        Set<OngoingJob> ongoingJobs = onDemandStatus.getMyOngoingJobs();

        assertThat(ongoingJobs.size()).isEqualTo(1);

        OngoingJob ongoingJob = ongoingJobs.iterator().next();
        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getTableReference().getKeyspace()).isEqualTo(KEYSPACE_NAME);
        assertThat(ongoingJob.getTableReference().getTable()).isEqualTo(TABLE_NAME);
        assertThat(ongoingJob.getTokenMapHash()).isEqualTo(hashValue);
        assertThat(ongoingJob.getRepiaredTokens()).isEqualTo(repairedTokens);
    }
}