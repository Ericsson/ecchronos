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
package com.ericsson.bss.cassandra.ecchronos.core;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import java.util.concurrent.ScheduledExecutorService;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwareStatement;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;

/**
 * Represents a container for builder configurations and state for the CASLockStatement.
 * This class is used to decouple builder fields from CASLock to avoid excessive field count.
 */
public class CASLockStatement
{
    private static final String COLUMN_RESOURCE = "resource";
    static final String COLUMN_NODE = "node";
    private static final String COLUMN_METADATA = "metadata";
    static final String COLUMN_PRIORITY = "priority";
    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private final PreparedStatement myCompeteStatement;
    private final PreparedStatement myLockStatement;
    private final PreparedStatement myRemoveLockStatement;
    private final PreparedStatement myUpdateLockStatement;
    private final PreparedStatement myRemoveLockPriorityStatement;
    private final PreparedStatement myGetPriorityStatement;
    private final StatementDecorator myStatementDecorator;

    private final boolean myRemoteRouting;
    private final String myKeyspaceName;
    private final CASLockFactoryCacheContext myCasLockFactoryCacheContext;
    private final ScheduledExecutorService myExecutor;
    private final ConsistencyLevel mySerialConsistencyLevel;
    private final CqlSession mySession;

    public CASLockStatement(
        final Boolean remoteRouting,
        final String keyspaceName,
        final ScheduledExecutorService executor,
        final ConsistencyLevel seriaConsistencyLevel,
        final CqlSession session,
        final CASLockFactoryCacheContext casLockFactoryCacheContext,
        final StatementDecorator statementDecorator)
    {
        myRemoteRouting = remoteRouting;
        myKeyspaceName = keyspaceName;
        myExecutor = executor;
        mySerialConsistencyLevel = seriaConsistencyLevel;
        mySession = session;
        myCasLockFactoryCacheContext = casLockFactoryCacheContext;
        myStatementDecorator = statementDecorator;

        myCompeteStatement = mySession.prepare(competeStatement());
        myLockStatement = mySession.prepare((insertLockStatement()));
        myRemoveLockStatement = mySession.prepare(removeLockStatement());
        myUpdateLockStatement = mySession.prepare((updateLockStatement()));
        myRemoveLockPriorityStatement = mySession.prepare(removeLockPriorityStatement());
        myGetPriorityStatement = mySession.prepare(getPriorityStatement());
    }

    public final ResultSet execute(final String dataCenter, final BoundStatement statement)
    {
        Statement executeStatement;

        if (dataCenter != null && myRemoteRouting)
        {
            executeStatement = new DataCenterAwareStatement(statement, dataCenter);
        }
        else
        {
            executeStatement = statement;
        }

        return mySession.execute(myStatementDecorator.apply(executeStatement));
    }

    private SimpleStatement insertLockStatement()
    {
        SimpleStatement insertLockStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK)
                    .value(COLUMN_RESOURCE, bindMarker())
                    .value(COLUMN_NODE, bindMarker())
                    .value(COLUMN_METADATA, bindMarker())
                    .ifNotExists()
                    .build()
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                    .setSerialConsistencyLevel(mySerialConsistencyLevel);
        return insertLockStatement;
    }

    private SimpleStatement removeLockStatement()
    {
        SimpleStatement removeLockStatement = QueryBuilder.deleteFrom(myKeyspaceName, TABLE_LOCK)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(mySerialConsistencyLevel);
        return removeLockStatement;
    }

    private SimpleStatement updateLockStatement()
    {
        SimpleStatement updateLockStatement = QueryBuilder.update(myKeyspaceName, TABLE_LOCK)
                .setColumn(COLUMN_NODE, bindMarker())
                .setColumn(COLUMN_METADATA, bindMarker())
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(mySerialConsistencyLevel);
        return updateLockStatement;
    }

    private SimpleStatement competeStatement()
    {
        SimpleStatement competeStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_PRIORITY, bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return competeStatement;
    }

    private SimpleStatement getPriorityStatement()
    {
        SimpleStatement priorityStatement = QueryBuilder.selectFrom(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .columns(COLUMN_PRIORITY, COLUMN_NODE)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return priorityStatement;
    }

    private SimpleStatement removeLockPriorityStatement()
    {
        SimpleStatement removeLockPriorityStatement = QueryBuilder.deleteFrom(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return removeLockPriorityStatement;
    }

    public final PreparedStatement getCompeteStatement()
    {
        return myCompeteStatement;
    }

    public final PreparedStatement getLockStatement()
    {
        return myLockStatement;
    }

    public final PreparedStatement getRemoveLockStatement()
    {
        return myRemoveLockStatement;
    }

    public final PreparedStatement getUpdateLockStatement()
    {
        return myUpdateLockStatement;
    }

    public final PreparedStatement getRemoveLockPriorityStatement()
    {
        return myRemoveLockPriorityStatement;
    }

    public final PreparedStatement getGetPriorityStatement()
    {
        return myGetPriorityStatement;
    }

    public final boolean isRemoteRouting()
    {
        return myRemoteRouting;
    }

    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    public final CASLockFactoryCacheContext getCasLockFactoryCacheContext()
    {
        return myCasLockFactoryCacheContext;
    }

    public final ScheduledExecutorService getExecutor()
    {
        return myExecutor;
    }

    public final ConsistencyLevel getSerialConsistencyLevel()
    {
        return mySerialConsistencyLevel;
    }

    public final CqlSession getSession()
    {
        return mySession;
    }

    public final StatementDecorator getStatementDecorator()
    {
        return myStatementDecorator;
    }
}
