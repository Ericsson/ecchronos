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

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ConsistencyType;

/**
 * Represents a container for builder configurations and state for the CASLockFactory.
 * This class is used to decouple builder fields from CASLockFactory to avoid excessive field count.
 */
public class CASLockFactoryBuilder
{
    private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";
    private static final long DEFAULT_EXPIRY_TIME_IN_SECONDS = 30L;
    private static final ConsistencyType DEFAULT_CONSISTENCY_SERIAL = ConsistencyType.DEFAULT;

    private NativeConnectionProvider myNativeConnectionProvider;
    private HostStates myHostStates;
    private StatementDecorator myStatementDecorator;
    private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
    private long myCacheExpiryTimeInSeconds = DEFAULT_EXPIRY_TIME_IN_SECONDS;
    private ConsistencyType myConsistencyType = DEFAULT_CONSISTENCY_SERIAL;

    public final CASLockFactoryBuilder withNativeConnectionProvider(
        final NativeConnectionProvider nativeConnectionProvider)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        return this;
    }

    public final CASLockFactoryBuilder withHostStates(final HostStates hostStates)
    {
        myHostStates = hostStates;
        return this;
    }

    public final CASLockFactoryBuilder withStatementDecorator(final StatementDecorator statementDecorator)
    {
        myStatementDecorator = statementDecorator;
        return this;
    }

    public final CASLockFactoryBuilder withKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
        return this;
    }

    public final CASLockFactoryBuilder withCacheExpiryInSeconds(final long cacheExpiryInSeconds)
    {
        myCacheExpiryTimeInSeconds = cacheExpiryInSeconds;
        return this;
    }

    public final CASLockFactoryBuilder withConsistencySerial(final ConsistencyType consistencyType)
    {
        myConsistencyType = consistencyType;
        return this;
    }

    public final CASLockFactory build()
    {
        if (myNativeConnectionProvider == null)
        {
            throw new IllegalArgumentException("Native connection provider cannot be null");
        }

        if (myHostStates == null)
        {
            throw new IllegalArgumentException("Host states cannot be null");
        }

        if (myStatementDecorator == null)
        {
            throw new IllegalArgumentException("Statement decorator cannot be null");
        }

        return new CASLockFactory(this);
    }

    public final NativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    public final HostStates getHostStates()
    {
        return myHostStates;
    }

    public final StatementDecorator getStatementDecorator()
    {
        return myStatementDecorator;
    }

    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    public final long getCacheExpiryTimeInSecond()
    {
        return myCacheExpiryTimeInSeconds;
    }

    public final ConsistencyType getConsistencyType()
    {
        return myConsistencyType;
    }

}
