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

import java.util.concurrent.ScheduledExecutorService;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ConsistencyType;

/**
 * Represents a container for builder configurations and state for the CASLockStatement.
 * This class is used to decouple builder fields from CASLock to avoid excessive field count.
 */
public class CASLockProperties
{
    private final boolean myRemoteRouting;
    private final String myKeyspaceName;
    private final ScheduledExecutorService myExecutor;
    private final ConsistencyLevel mySerialConsistencyLevel;
    private final CqlSession mySession;
    private final StatementDecorator myStatementDecorator;

    CASLockProperties(
        final Boolean remoteRouting,
        final String keyspaceName,
        final ScheduledExecutorService executor,
        final ConsistencyType consistencyType,
        final CqlSession session,
        final StatementDecorator statementDecorator)
    {
        myRemoteRouting = remoteRouting;
        myKeyspaceName = keyspaceName;
        myExecutor = executor;
        mySerialConsistencyLevel = defineSerialConsistencyLevel(consistencyType);
        mySession = session;
        myStatementDecorator = statementDecorator;
    }

    /**
     * Defines the serial consistency level for CAS operations.
     * @param consistencyType the consistency type
     * @return the serial consistency level
     */
    public final ConsistencyLevel defineSerialConsistencyLevel(final ConsistencyType consistencyType)
    {
        ConsistencyLevel serialConsistencyLevel;

        if (ConsistencyType.DEFAULT.equals(consistencyType))
        {
            serialConsistencyLevel = myRemoteRouting
                ? ConsistencyLevel.LOCAL_SERIAL
                : ConsistencyLevel.SERIAL;
        }
        else
        {
            serialConsistencyLevel = ConsistencyType.LOCAL.equals(consistencyType)
                ? ConsistencyLevel.LOCAL_SERIAL
                : ConsistencyLevel.SERIAL;
        }
        return serialConsistencyLevel;
    }

    /**
     * Returns whether remote routing.
     * @return true if remote routing
     */
    public final boolean isRemoteRouting()
    {
        return myRemoteRouting;
    }

    /**
     * Returns the keyspace name.
     * @return the keyspace name
     */
    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    /**
     * Returns the executor.
     * @return the executor
     */
    public final ScheduledExecutorService getExecutor()
    {
        return myExecutor;
    }

    /**
     * Returns the serial consistency level.
     * @return the serial consistency level
     */
    public final ConsistencyLevel getSerialConsistencyLevel()
    {
        return mySerialConsistencyLevel;
    }

    /**
     * Returns the session.
     * @return the session
     */
    public final CqlSession getSession()
    {
        return mySession;
    }

    /**
     * Returns the statement decorator.
     * @return the statement decorator
     */
    public final StatementDecorator getStatementDecorator()
    {
        return myStatementDecorator;
    }

}
