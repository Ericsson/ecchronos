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

import java.util.Map;
import java.util.UUID;

/**
 * Represents a container for cache-related configurations and state for the CASLockFactory.
 * This class is used to decouple cache-related fields from CASLockFactory to avoid excessive field count.
 */
public final class CASLockFactoryCacheContext
{
    private final Map<UUID, LockCache> myLockCache;
    private final long myLockUpdateTimeInSeconds;
    private final int myFailedLockRetryAttempts;

    /**
     * Constructs a CASLockFactoryCacheContext from a builder.
     *
     * @param builder the builder containing configuration values.
     */
    public CASLockFactoryCacheContext(final Builder builder)
    {
        myLockCache = builder.myLockCache;
        myLockUpdateTimeInSeconds = builder.myLockUpdateTimeInSeconds;
        myFailedLockRetryAttempts = builder.myFailedLockRetryAttempts;
    }

    /**
     * Gets the lock cache for the specified node.
     *
     * @param uuid the node identifier.
     * @return the lock cache for the node, or null if not present.
     */
    public LockCache getLockCache(final UUID uuid)
    {
        return myLockCache.get(uuid);
    }

    /**
     * Adds a lock cache for the specified node if not already present.
     *
     * @param uuid the node identifier.
     * @param lockCache the lock cache to add.
     */
    public void addLockCache(final UUID uuid, final LockCache lockCache)
    {
        myLockCache.putIfAbsent(uuid, lockCache);
    }

    /**
     * Removes the lock cache for the specified node.
     *
     * @param uuid the node identifier.
     */
    public void removeLockCache(final UUID uuid)
    {
        myLockCache.remove(uuid);
    }

    /**
     * Gets the lock update time interval in seconds.
     *
     * @return the lock update time in seconds.
     */
    public long getLockUpdateTimeInSeconds()
    {
        return myLockUpdateTimeInSeconds;
    }

    /**
     * Gets the number of retry attempts for failed lock acquisitions.
     *
     * @return the number of failed lock retry attempts.
     */
    public int getFailedLockRetryAttempts()
    {
        return myFailedLockRetryAttempts;
    }

    /**
     * Creates a new builder for constructing {@link CASLockFactoryCacheContext} instances.
     *
     * @return a new builder.
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }


    /**
     * Builder for constructing {@link CASLockFactoryCacheContext} instances.
     */
    public static class Builder
    {
        private Map<UUID, LockCache> myLockCache;
        private int myLockUpdateTimeInSeconds;
        private int myFailedLockRetryAttempts;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Sets the lock update time in seconds.
         *
         * @param lockTimeInSeconds the lock update time.
         * @return this builder.
         */
        public final Builder withLockUpdateTimeInSeconds(final int lockTimeInSeconds)
        {
            myLockUpdateTimeInSeconds = lockTimeInSeconds;
            return this;
        }

        /**
         * Sets the number of retry attempts for failed lock acquisitions.
         *
         * @param failedLockRetryAttempts the number of retry attempts.
         * @return this builder.
         */
        public final Builder withFailedLockRetryAttempts(final int failedLockRetryAttempts)
        {
            myFailedLockRetryAttempts = failedLockRetryAttempts;
            return this;
        }

        /**
         * Sets the lock cache map.
         *
         * @param lockCache the lock cache map keyed by node UUID.
         * @return this builder.
         */
        public final Builder withLockCache(final Map<UUID, LockCache> lockCache)
        {
            myLockCache = lockCache;
            return this;
        }

        /**
         * Builds the {@link CASLockFactoryCacheContext} instance.
         *
         * @return the constructed context.
         */
        public final CASLockFactoryCacheContext build()
        {
            return new CASLockFactoryCacheContext(this);
        }
    }
}
