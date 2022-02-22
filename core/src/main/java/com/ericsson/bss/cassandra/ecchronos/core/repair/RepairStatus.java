/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

public class RepairStatus
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairStatus.class);

    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "repair_status";
    private static final String HOST_ID_COLUMN_NAME = "host_id";
    private static final String STATUS_COLUMN_NAME = "status";
    private static final String REPAIR_ID_COLUMN_NAME = "repair_id";
    private static final String TABLE_REFERENCE_COLUMN_NAME = "table_reference";
    private static final String TRIGGERED_BY_COLUMN_NAME = "triggered_by";
    private static final String UDT_TABLE_REFERENCE_NAME = "table_reference";
    private static final String UDT_ID_NAME = "id";
    private static final String UDT_KEYSPACE_NAME = "keyspace_name";
    private static final String UDT_TABLE_NAME = "table_name";
    private static final String FINISHED_AT_COLUMN_NAME = "finished_at";
    private static final String STARTED_AT_COLUMN_NAME = "started_at";
    private static final String REMAINING_TASKS_COLUMN_NAME = "remaining_tasks";

    private final Session mySession;
    private final UUID myHostId;
    private final UserType myUDTTableReferenceType;
    private final PreparedStatement myGetStatusStatement;
    private final PreparedStatement myInsertNewJobStatement;
    private final PreparedStatement myUpdateJobWithRemainingTasks;
    private final PreparedStatement myUpdateJobToFinishedStatement;
    private final TableReferenceFactory myTableReferenceFactory;

    public RepairStatus(NativeConnectionProvider nativeConnectionProvider)
    {
        mySession = nativeConnectionProvider.getSession();
        myHostId = nativeConnectionProvider.getLocalHost().getHostId();
        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession.getCluster().getMetadata());
        myUDTTableReferenceType =
                mySession.getCluster().getMetadata().getKeyspace(KEYSPACE_NAME).getUserType(UDT_TABLE_REFERENCE_NAME);

        BuiltStatement
                getStatusStatement =
                select().from(KEYSPACE_NAME, TABLE_NAME).where(eq(HOST_ID_COLUMN_NAME, bindMarker()));
        BuiltStatement insertNewJobStatement =
                insertInto(KEYSPACE_NAME, TABLE_NAME).value(HOST_ID_COLUMN_NAME, bindMarker())
                        .value(REPAIR_ID_COLUMN_NAME, bindMarker()).value(TABLE_REFERENCE_COLUMN_NAME, bindMarker())
                        .value(TRIGGERED_BY_COLUMN_NAME, bindMarker()).value(REMAINING_TASKS_COLUMN_NAME, bindMarker())
                        .value(STARTED_AT_COLUMN_NAME, bindMarker()).value(STATUS_COLUMN_NAME, "started");
        BuiltStatement updateJobWithRemainingTasks =
                update(KEYSPACE_NAME, TABLE_NAME).with(set(REMAINING_TASKS_COLUMN_NAME, bindMarker()))
                        .where(eq(HOST_ID_COLUMN_NAME, bindMarker()))
                        .and(eq(REPAIR_ID_COLUMN_NAME, bindMarker()));
        BuiltStatement updateJobToFinishedStatement =
                update(KEYSPACE_NAME, TABLE_NAME).with(set(STATUS_COLUMN_NAME, "finished")).and(set(
                                FINISHED_AT_COLUMN_NAME, bindMarker())).and(set(REMAINING_TASKS_COLUMN_NAME, 0))
                        .where(eq(HOST_ID_COLUMN_NAME, bindMarker()))
                        .and(eq(REPAIR_ID_COLUMN_NAME, bindMarker()));

        myGetStatusStatement = mySession.prepare(getStatusStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myInsertNewJobStatement =
                mySession.prepare(insertNewJobStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myUpdateJobWithRemainingTasks =
                mySession.prepare(updateJobWithRemainingTasks).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        myUpdateJobToFinishedStatement =
                mySession.prepare(updateJobToFinishedStatement).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    public Set<OngoingRepair> getUnfinishedOnDemandRepairs()
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(myHostId));
        Set<OngoingRepair> ongoingRepairs = new HashSet<>();
        for(Row row : result.all())
        {
            try
            {
                OngoingRepair.Status status = OngoingRepair.Status.valueOf(row.getString(STATUS_COLUMN_NAME));
                String triggeredBy = row.getString(TRIGGERED_BY_COLUMN_NAME);
                boolean triggeredByUser = "user".equals(triggeredBy);
                if(status.equals(OngoingRepair.Status.started) && triggeredByUser)
                {
                    createOngoingRepair(ongoingRepairs, row, status, triggeredBy);
                }
            }
            catch(IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {}, unable to parse status or triggeredBy",
                        row.getUUID(REPAIR_ID_COLUMN_NAME));
                continue;
            }

        }
        return ongoingRepairs;
    }

    public Set<OngoingRepair> getUnfinishedRepairs()
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(myHostId));
        Set<OngoingRepair> ongoingRepairs = new HashSet<>();
        for(Row row : result.all())
        {
            try
            {
                OngoingRepair.Status status = OngoingRepair.Status.valueOf(row.getString(STATUS_COLUMN_NAME));
                String triggeredBy = row.getString(TRIGGERED_BY_COLUMN_NAME);
                if(status.equals(OngoingRepair.Status.started))
                {
                    createOngoingRepair(ongoingRepairs, row, status, triggeredBy);
                }
            }
            catch(IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {}, unable to parse status or triggeredBy",
                        row.getUUID(REPAIR_ID_COLUMN_NAME));
                continue;
            }

        }
        return ongoingRepairs;
    }

    public Set<OngoingRepair> getAllRepairs()
    {
        ResultSet result = mySession.execute(myGetStatusStatement.bind(myHostId));
        Set<OngoingRepair> ongoingRepairs = new HashSet<>();
        for(Row row : result.all())
        {
            try
            {
                OngoingRepair.Status status = OngoingRepair.Status.valueOf(row.getString(STATUS_COLUMN_NAME));
                String triggeredBy = row.getString(TRIGGERED_BY_COLUMN_NAME);
                createOngoingRepair(ongoingRepairs, row, status, triggeredBy);
            }
            catch(IllegalArgumentException e)
            {
                LOG.warn("Ignoring table repair job with id {}, unable to parse status or triggeredBy",
                        row.getUUID(REPAIR_ID_COLUMN_NAME));
                continue;
            }

        }
        return ongoingRepairs;
    }

    private void createOngoingRepair(Set<OngoingRepair> ongoingRepairs, Row row, OngoingRepair.Status status, String triggeredBy)
    {
        UUID repairId = row.getUUID(REPAIR_ID_COLUMN_NAME);
        UDTValue uDTTableReference = row.getUDTValue(TABLE_REFERENCE_COLUMN_NAME);
        String keyspace = uDTTableReference.getString(UDT_KEYSPACE_NAME);
        String table = uDTTableReference.getString(UDT_TABLE_NAME);
        TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
        Long finshedAt = row.get(FINISHED_AT_COLUMN_NAME, Long.class);
        Long startedAt = row.get(STARTED_AT_COLUMN_NAME, Long.class);
        int remaningTasks = row.get(REMAINING_TASKS_COLUMN_NAME, int.class);

        if(uDTTableReference.getUUID(UDT_ID_NAME).equals(tableReference.getId()))
        {
            OngoingRepair ongoingRepair = new OngoingRepair.Builder()
                    .withTableReference(tableReference)
                    .withOngoingRepairInfo(repairId, status, finshedAt, startedAt, triggeredBy, remaningTasks)
                    .build();
            ongoingRepairs.add(ongoingRepair);
        }
        else
        {
            LOG.info("Ignoring table repair job with id {} of table {} as it was for table {}.{}({})", repairId,
                    tableReference, keyspace, table, uDTTableReference.getUUID(UDT_ID_NAME));
        }
    }

    public void startRepair(UUID repairId, TableReference tableReference, String triggeredBy, int tasks, long startTime)
    {
        UDTValue uDTTableReference = myUDTTableReferenceType.newValue().setUUID(UDT_ID_NAME, tableReference.getId())
                .setString(UDT_KEYSPACE_NAME, tableReference.getKeyspace())
                .setString(UDT_TABLE_NAME, tableReference.getTable());
        BoundStatement statement = myInsertNewJobStatement.bind(myHostId, repairId, uDTTableReference,
                triggeredBy, tasks, startTime);
        mySession.execute(statement);
    }

    public void updateWithRemainingTasks(UUID repairId, int tasks)
    {
        mySession.execute(myUpdateJobWithRemainingTasks.bind(tasks, myHostId, repairId));
    }

    public void finishRepair(UUID repairId)
    {
        mySession.execute(myUpdateJobToFinishedStatement.bind(System.currentTimeMillis(), myHostId, repairId));
    }
}
