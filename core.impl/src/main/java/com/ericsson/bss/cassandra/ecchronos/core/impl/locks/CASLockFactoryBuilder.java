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

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;

/**
 * Represents a container for builder configurations and state for the CASLockFactory.
 * This class is used to decouple builder fields from CASLockFactory to avoid excessive field count.
 */
public class CASLockFactoryBuilder
{
    private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";
    private static final long DEFAULT_EXPIRY_TIME_IN_SECONDS = 30L;
    private static final ConsistencyType DEFAULT_CONSISTENCY_SERIAL = ConsistencyType.SERIAL;

    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
    private long myCacheExpiryTimeInSeconds = DEFAULT_EXPIRY_TIME_IN_SECONDS;
    private ConsistencyType myConsistencyType = DEFAULT_CONSISTENCY_SERIAL;

    /**
     * Sets the native connection provider.
     *
     * @param nativeConnectionProvider the distributed native connection provider.
     * @return this builder.
     */
    public final CASLockFactoryBuilder withNativeConnectionProvider(final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        return this;
    }

    /**
     * Sets the keyspace name for lock tables.
     *
     * @param keyspaceName the keyspace name.
     * @return this builder.
     */
    public final CASLockFactoryBuilder withKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
        return this;
    }

    /**
     * Sets the cache expiry time in seconds for lock caching.
     *
     * @param cacheExpiryInSeconds the cache expiry time in seconds.
     * @return this builder.
     */
    public final CASLockFactoryBuilder withCacheExpiryInSeconds(final long cacheExpiryInSeconds)
    {
        myCacheExpiryTimeInSeconds = cacheExpiryInSeconds;
        return this;
    }

    /**
     * Sets the serial consistency type for lock operations.
     *
     * @param consistencyType the consistency type (LOCAL or SERIAL).
     * @return this builder.
     */
    public final CASLockFactoryBuilder withConsistencySerial(final ConsistencyType consistencyType)
    {
        myConsistencyType = consistencyType;
        return this;
    }

    /**
     * Builds the {@link CASLockFactory} instance.
     *
     * @return the constructed CAS lock factory.
     * @throws IllegalArgumentException if native connection provider is null.
     */
    public final CASLockFactory build()
    {
        if (myNativeConnectionProvider == null)
        {
            throw new IllegalArgumentException("Native connection provider cannot be null");
        }

        return new CASLockFactory(this);
    }

    /**
     * Gets the configured native connection provider.
     *
     * @return the native connection provider.
     */
    public final DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    /**
     * Gets the configured keyspace name.
     *
     * @return the keyspace name.
     */
    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    /**
     * Gets the configured cache expiry time in seconds.
     *
     * @return the cache expiry time in seconds.
     */
    public final long getCacheExpiryTimeInSecond()
    {
        return myCacheExpiryTimeInSeconds;
    }

    /**
     * Gets the configured consistency type.
     *
     * @return the consistency type.
     */
    public final ConsistencyType getConsistencyType()
    {
        return myConsistencyType;
    }
}
