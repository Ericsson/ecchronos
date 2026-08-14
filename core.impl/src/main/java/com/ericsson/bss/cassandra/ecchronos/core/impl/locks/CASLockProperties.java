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

import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.util.concurrent.ScheduledExecutorService;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Represents a container for builder configurations and state for the CASLockStatement.
 * This class is used to decouple builder fields from CASLock to avoid excessive field count.
 */
public class CASLockProperties
{
    private final ConnectionType myConnectionType;
    private final String myKeyspaceName;
    private final ScheduledExecutorService myExecutor;
    private final ConsistencyLevel mySerialConsistencyLevel;
    private final CqlSession mySession;

    CASLockProperties(final ConnectionType connectionType,
                      final String keyspaceName,
                      final ScheduledExecutorService executor,
                      final ConsistencyType consistencyType,
                      final CqlSession session)
    {
        myConnectionType = connectionType;
        myKeyspaceName = keyspaceName;
        myExecutor = executor;
        mySerialConsistencyLevel = defineSerialConsistencyLevel(consistencyType);
        mySession = session;
    }

    /**
     * Determines the serial consistency level based on the consistency type.
     *
     * @param consistencyType the consistency type (LOCAL or SERIAL).
     * @return the corresponding serial consistency level.
     */
    public final ConsistencyLevel defineSerialConsistencyLevel(final ConsistencyType consistencyType)
    {
        ConsistencyLevel serialConsistencyLevel = ConsistencyType.LOCAL.equals(consistencyType)
                ? ConsistencyLevel.LOCAL_SERIAL
                : ConsistencyLevel.SERIAL;
        return serialConsistencyLevel;
    }

    /**
     * Gets the keyspace name used for lock tables.
     *
     * @return the keyspace name.
     */
    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    /**
     * Gets the scheduled executor service for lock refresh tasks.
     *
     * @return the scheduled executor service.
     */
    public final ScheduledExecutorService getExecutor()
    {
        return myExecutor;
    }

    /**
     * Gets the serial consistency level for lock operations.
     *
     * @return the serial consistency level.
     */
    public final ConsistencyLevel getSerialConsistencyLevel()
    {
        return mySerialConsistencyLevel;
    }

    /**
     * Gets the CQL session used for lock operations.
     *
     * @return the CQL session.
     */
    public final CqlSession getSession()
    {
        return mySession;
    }

    /**
     * Checks if this instance is configured for datacenter-aware agent type.
     *
     * @return true if the connection type is datacenter-aware, false otherwise.
     */
    public final boolean isDatacenterAwareAgentType()

    {
        return myConnectionType == ConnectionType.datacenterAware;
    }
}
