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

    /** Constructs a new CASLockFactoryBuilder. */
    public CASLockFactoryBuilder()
    {
        // Default constructor
    }

    /**
     * Sets the native connection provider.
     * @param nativeConnectionProvider the native connection provider
     * @return this builder
     */
    public final CASLockFactoryBuilder withNativeConnectionProvider(
        final NativeConnectionProvider nativeConnectionProvider)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        return this;
    }

    /**
     * Sets the host states.
     * @param hostStates the host states
     * @return this builder
     */
    public final CASLockFactoryBuilder withHostStates(final HostStates hostStates)
    {
        myHostStates = hostStates;
        return this;
    }

    /**
     * Sets the statement decorator.
     * @param statementDecorator the statement decorator
     * @return this builder
     */
    public final CASLockFactoryBuilder withStatementDecorator(final StatementDecorator statementDecorator)
    {
        myStatementDecorator = statementDecorator;
        return this;
    }

    /**
     * Sets the keyspace name.
     * @param keyspaceName the keyspace name
     * @return this builder
     */
    public final CASLockFactoryBuilder withKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
        return this;
    }

    /**
     * Sets the cache expiry in seconds.
     * @param cacheExpiryInSeconds the cache expiry in seconds
     * @return this builder
     */
    public final CASLockFactoryBuilder withCacheExpiryInSeconds(final long cacheExpiryInSeconds)
    {
        myCacheExpiryTimeInSeconds = cacheExpiryInSeconds;
        return this;
    }

    /**
     * Sets the consistency serial.
     * @param consistencyType the consistency type
     * @return this builder
     */
    public final CASLockFactoryBuilder withConsistencySerial(final ConsistencyType consistencyType)
    {
        myConsistencyType = consistencyType;
        return this;
    }

    /**
     * Builds and returns the instance.
     * @return the built instance
     */
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

    /**
     * Returns the native connection provider.
     * @return the native connection provider
     */
    public final NativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    /**
     * Returns the host states.
     * @return the host states
     */
    public final HostStates getHostStates()
    {
        return myHostStates;
    }

    /**
     * Returns the statement decorator.
     * @return the statement decorator
     */
    public final StatementDecorator getStatementDecorator()
    {
        return myStatementDecorator;
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
     * Returns the cache expiry time in second.
     * @return the cache expiry time in second
     */
    public final long getCacheExpiryTimeInSecond()
    {
        return myCacheExpiryTimeInSeconds;
    }

    /**
     * Returns the consistency type.
     * @return the consistency type
     */
    public final ConsistencyType getConsistencyType()
    {
        return myConsistencyType;
    }

}
