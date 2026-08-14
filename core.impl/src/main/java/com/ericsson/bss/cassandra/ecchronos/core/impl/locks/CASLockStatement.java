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
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

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

    /**
     * Constructs a CASLockStatement, preparing all necessary CQL statements for lock operations.
     *
     * @param casLockProperties the lock properties containing session and keyspace information.
     * @param casLockFactoryCacheContext the cache context for lock operations.
     */
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

    /**
     * Executes the given bound statement against the CQL session.
     *
     * @param statement the bound statement to execute.
     * @return the result set from the execution.
     */
    public final ResultSet execute(final BoundStatement statement)
    {
        return myCasLockProperties.getSession().execute(statement);
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

    /**
     * Gets the prepared statement for competing for a lock priority.
     *
     * @return the compete prepared statement.
     */
    public final PreparedStatement getCompeteStatement()
    {
        return myCompeteStatement;
    }

    /**
     * Gets the prepared statement for inserting a lock.
     *
     * @return the lock insert prepared statement.
     */
    public final PreparedStatement getLockStatement()
    {
        return myLockStatement;
    }

    /**
     * Gets the prepared statement for removing a lock.
     *
     * @return the remove lock prepared statement.
     */
    public final PreparedStatement getRemoveLockStatement()
    {
        return myRemoveLockStatement;
    }

    /**
     * Gets the prepared statement for updating a lock.
     *
     * @return the update lock prepared statement.
     */
    public final PreparedStatement getUpdateLockStatement()
    {
        return myUpdateLockStatement;
    }

    /**
     * Gets the prepared statement for removing a lock priority entry.
     *
     * @return the remove lock priority prepared statement.
     */
    public final PreparedStatement getRemoveLockPriorityStatement()
    {
        return myRemoveLockPriorityStatement;
    }

    /**
     * Gets the prepared statement for retrieving lock priorities.
     *
     * @return the get priority prepared statement.
     */
    public final PreparedStatement getGetPriorityStatement()
    {
        return myGetPriorityStatement;
    }

    /**
     * Gets the prepared statement for retrieving lock metadata.
     *
     * @return the lock metadata prepared statement.
     */
    public final PreparedStatement getLockMetadataStatement()
    {
        return myGetLockMetadataStatement;
    }

    /**
     * Gets the lock factory cache context.
     *
     * @return the CAS lock factory cache context.
     */
    public final CASLockFactoryCacheContext getCasLockFactoryCacheContext()
    {
        return myCasLockFactoryCacheContext;
    }

    /**
     * Gets the lock properties.
     *
     * @return the CAS lock properties.
     */
    public final CASLockProperties getCasLockProperties()
    {
        return myCasLockProperties;
    }
}
