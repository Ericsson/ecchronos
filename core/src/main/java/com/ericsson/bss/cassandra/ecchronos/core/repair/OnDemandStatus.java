package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class OnDemandStatus
{

	private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "ondemand_status";
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
    private static final String UDT_KAYSPACE_NAME = "keyspace_name";
    private static final String UDT_TABLE_NAME = "table_name";

    private final Session mySession;
    private final UUID myHostId;
    private final UserType myUDTTokenType;
    private final UserType myUDTTableReferenceType;
    private final PreparedStatement myGetStatusStatement;
    private final PreparedStatement myInsertNewJobStatement;
    private final PreparedStatement myUpdateRepairedTokenForJobStatement;
    private final PreparedStatement myUpdateJobToFinishedStatement;
    private final PreparedStatement myUpdateJobToFailedStatement;
    private final TableReferenceFactory myTableReferenceFactory;

    public class OngoingJob
    {
        private final UUID myJobId;
        private final TableReference myTableReference;
        private final int myTokenMapHash;
        private final Set<UDTValue> myRepiaredTokens;

        public OngoingJob(UUID jobId, TableReference tableReference, int tokenMapHash, Set<UDTValue> repiaredTokens)
        {
            myJobId = jobId;
            myTableReference = tableReference;
            myTokenMapHash = tokenMapHash;
            myRepiaredTokens = repiaredTokens;
        }

        public UUID getJobId()
        {
            return myJobId;
        }

        public TableReference getTableReference()
        {
            return myTableReference;
        }

        public int getTokenMapHash()
        {
            return myTokenMapHash;
        }

        public Set<UDTValue> getRepiaredTokens()
        {
            return myRepiaredTokens;
        }
    }

    public OnDemandStatus(NativeConnectionProvider nativeConnectionProvider)
    {
        mySession = nativeConnectionProvider.getSession();
        myHostId = nativeConnectionProvider.getLocalHost().getHostId();
        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession.getCluster().getMetadata());
        myUDTTokenType = mySession.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME).getUserType(UDT_TOKEN_RANGE_NAME);
        myUDTTableReferenceType = mySession.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME).getUserType(UDT_TABLE_REFERENCE_NAME);

        BuiltStatement getStatusStatement = select().from(KEYSPACE_NAME, TABLE_NAME).where(eq(HOST_ID_COLUMN_NAME, bindMarker()));
        BuiltStatement insertNewJobStatement = insertInto(KEYSPACE_NAME, TABLE_NAME).value(HOST_ID_COLUMN_NAME, bindMarker()).value(JOB_ID_COLUMN_NAME, bindMarker()).value(TABLE_REFERENCE_COLUMN_NAME, bindMarker()).value(TOKEN_MAP_HASH_COLUMN_NAME, bindMarker()).value(STATUS_COLUMN_NAME, "started");
        BuiltStatement updateRepairedTokenForJobStatement = update(KEYSPACE_NAME, TABLE_NAME).with(set(REPAIRED_TOKENS_COLUMN_NAME, bindMarker())).where(eq(HOST_ID_COLUMN_NAME, bindMarker())).and(eq(JOB_ID_COLUMN_NAME, bindMarker()));
        BuiltStatement updateJobToFinishedStatement = update(KEYSPACE_NAME, TABLE_NAME).with(set(STATUS_COLUMN_NAME, "finished")).where(eq(HOST_ID_COLUMN_NAME, bindMarker())).and(eq(JOB_ID_COLUMN_NAME, bindMarker()));
        BuiltStatement updateJobToFailedStatement = update(KEYSPACE_NAME, TABLE_NAME).with(set(STATUS_COLUMN_NAME, "failed")).where(eq(HOST_ID_COLUMN_NAME, bindMarker())).and(eq(JOB_ID_COLUMN_NAME, bindMarker()));

        myGetStatusStatement = mySession.prepare(getStatusStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myInsertNewJobStatement = mySession.prepare(insertNewJobStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myUpdateRepairedTokenForJobStatement = mySession.prepare(updateRepairedTokenForJobStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myUpdateJobToFinishedStatement = mySession.prepare(updateJobToFinishedStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myUpdateJobToFailedStatement = mySession.prepare(updateJobToFailedStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    public Set<OngoingJob> getMyOngoingJobs()
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(myHostId));

        Set<OngoingJob> ongoingJobs = new HashSet<>();
        for(Row row: result.all())
        {
            if("started".equals(row.getString(STATUS_COLUMN_NAME)))
            {
                UUID jobId = row.getUUID(JOB_ID_COLUMN_NAME);
                int tokenMapHash = row.getInt(TOKEN_MAP_HASH_COLUMN_NAME);
                Set<UDTValue> repiaredTokens = row.getSet(REPAIRED_TOKENS_COLUMN_NAME, UDTValue.class);
                UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
                String keyspace = uDTTableReference.getString(UDT_KAYSPACE_NAME);
                String table = uDTTableReference.getString(UDT_TABLE_NAME);
                TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);

                if(uDTTableReference.getUUID(UDT_ID_NAME).equals(tableReference.getId()))
                {
                    ongoingJobs.add(new OngoingJob(jobId, tableReference, tokenMapHash, repiaredTokens));
                }
                else
                {
                    // Should we do something her? log?
                }
            }
        }

        return ongoingJobs;
    }

    public void addNewJob(UUID jobId, TableReference tableReference, int tokenMapHash)
    {
        UDTValue uDTTableReference = myUDTTableReferenceType.newValue().setUUID(UDT_ID_NAME, tableReference.getId()).setString(UDT_KAYSPACE_NAME, tableReference.getKeyspace()).setString(UDT_TABLE_NAME, tableReference.getTable());
        BoundStatement statement = myInsertNewJobStatement.bind(myHostId, jobId, uDTTableReference, tokenMapHash);
        mySession.execute(statement);
    }

    public void updateJob(UUID jobId, Set<UDTValue> repairedTokens)
    {
        mySession.execute(myUpdateRepairedTokenForJobStatement.bind(repairedTokens, myHostId, jobId));
    }

    public void finishJob(UUID jobId)
    {
        mySession.execute(myUpdateJobToFinishedStatement.bind(myHostId, jobId));
    }

    public void failJob(UUID jobId)
    {
        mySession.execute(myUpdateJobToFailedStatement.bind(myHostId, jobId));
    }

    public UDTValue createUDTTokenRangeValue(Long start, Long end)
    {
        return myUDTTokenType.newValue().setString(UDT_START_TOKEN_NAME, start.toString()).setString(UDT_END_TOKEN_NAME, end.toString());
    }

    public long getStartTokenFrom(UDTValue t)
    {
        return Long.valueOf(t.getString(UDT_START_TOKEN_NAME));
    }

    public long getEndTokenFrom(UDTValue t)
    {
        return Long.valueOf(t.getString(UDT_END_TOKEN_NAME));
    }
}