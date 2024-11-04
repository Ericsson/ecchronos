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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwareStatement;

/**
 * Represents a container for builder configurations and state for the CASLockStatement.
 * This class is used to decouple builder fields from CASLock to avoid excessive field count.
 */
public class CASLockStatement
{
    static final String COLUMN_RESOURCE = "resource";
    static final String COLUMN_NODE = "node";
    static final String COLUMN_METADATA = "metadata";
    static final String COLUMN_PRIORITY = "priority";

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private final PreparedStatement myCompeteStatement;
    private final PreparedStatement myLockStatement;
    private final PreparedStatement myRemoveLockStatement;
    private final PreparedStatement myUpdateLockStatement;
    private final PreparedStatement myRemoveLockPriorityStatement;
    private final PreparedStatement myGetPriorityStatement;
    private final PreparedStatement myGetLockMetadataStatement;

    private final CASLockProperties myCasLockProperties;
    private final CASLockFactoryCacheContext myCasLockFactoryCacheContext;

    public CASLockStatement(
                            final CASLockProperties casLockProperties,
                            final CASLockFactoryCacheContext casLockFactoryCacheContext)
    {
        myCasLockProperties = casLockProperties;
        myCasLockFactoryCacheContext = casLockFactoryCacheContext;
        myCompeteStatement = myCasLockProperties.getSession().prepare(competeStatement());
        myLockStatement = myCasLockProperties.getSession().prepare((insertLockStatement()));
        myRemoveLockStatement = myCasLockProperties.getSession().prepare(removeLockStatement());
        myUpdateLockStatement = myCasLockProperties.getSession().prepare((updateLockStatement()));
        myRemoveLockPriorityStatement = myCasLockProperties.getSession().prepare(removeLockPriorityStatement());
        myGetPriorityStatement = myCasLockProperties.getSession().prepare(getPriorityStatement());
        myGetLockMetadataStatement = myCasLockProperties.getSession().prepare(lockMetadataStatement());
    }

    public final ResultSet execute(final String dataCenter, final BoundStatement statement)
    {
        Statement executeStatement;

        if (dataCenter != null)
        {
            executeStatement = new DataCenterAwareStatement(statement, dataCenter);
        }
        else
        {
            executeStatement = statement;
        }

        return myCasLockProperties.getSession()
                .execute(myCasLockProperties
                        .getStatementDecorator()
                        .apply(executeStatement));
    }

    private SimpleStatement insertLockStatement()
    {
        SimpleStatement insertLockStatement = QueryBuilder
                .insertInto(myCasLockProperties.getKeyspaceName(), TABLE_LOCK)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_METADATA, bindMarker())
                .ifNotExists()
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(myCasLockProperties.getSerialConsistencyLevel());
        return insertLockStatement;
    }

    private SimpleStatement removeLockStatement()
    {
        SimpleStatement removeLockStatement = QueryBuilder
                .deleteFrom(myCasLockProperties.getKeyspaceName(), TABLE_LOCK)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(myCasLockProperties.getSerialConsistencyLevel());
        return removeLockStatement;
    }

    private SimpleStatement updateLockStatement()
    {
        SimpleStatement updateLockStatement = QueryBuilder
                .update(myCasLockProperties.getKeyspaceName(), TABLE_LOCK)
                .setColumn(COLUMN_NODE, bindMarker())
                .setColumn(COLUMN_METADATA, bindMarker())
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(myCasLockProperties.getSerialConsistencyLevel());
        return updateLockStatement;
    }

    private SimpleStatement competeStatement()
    {
        SimpleStatement competeStatement = QueryBuilder
                .insertInto(myCasLockProperties.getKeyspaceName(), TABLE_LOCK_PRIORITY)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_PRIORITY, bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return competeStatement;
    }

    private SimpleStatement getPriorityStatement()
    {
        SimpleStatement priorityStatement = QueryBuilder
                .selectFrom(myCasLockProperties.getKeyspaceName(), TABLE_LOCK_PRIORITY)
                .columns(COLUMN_PRIORITY, COLUMN_NODE)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return priorityStatement;
    }

    private SimpleStatement removeLockPriorityStatement()
    {
        SimpleStatement removeLockPriorityStatement = QueryBuilder
                .deleteFrom(myCasLockProperties.getKeyspaceName(), TABLE_LOCK_PRIORITY)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE)
                .isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
        return removeLockPriorityStatement;
    }

    private SimpleStatement lockMetadataStatement()
    {
        SimpleStatement lockMetadataStatement = QueryBuilder
                .selectFrom(myCasLockProperties.getKeyspaceName(), TABLE_LOCK)
                .column(COLUMN_METADATA)
                .whereColumn(COLUMN_RESOURCE)
                .isEqualTo(bindMarker())
                .build()
                .setSerialConsistencyLevel(myCasLockProperties.getSerialConsistencyLevel());
        return lockMetadataStatement;
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

    public final PreparedStatement getLockMetadataStatement()
    {
        return myGetLockMetadataStatement;
    }

    public final CASLockFactoryCacheContext getCasLockFactoryCacheContext()
    {
        return myCasLockFactoryCacheContext;
    }

    public final CASLockProperties getCasLockProperties()
    {
        return myCasLockProperties;
    }

}
