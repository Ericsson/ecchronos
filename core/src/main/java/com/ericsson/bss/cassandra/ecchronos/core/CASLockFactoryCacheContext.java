/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

/**
 * Represents a container for cache-related configurations and state for the CASLockFactory.
 * This class is used to decouple cache-related fields from CASLockFactory to avoid excessive field count.
 */
public final class CASLockFactoryCacheContext
{
    private final LockCache myLockCache;
    private final long myLockUpdateTimeInSeconds;
    private final int myFailedLockRetryAttempts;

    public CASLockFactoryCacheContext(final Builder builder)
    {
        myLockCache = builder.myLockCache;
        myLockUpdateTimeInSeconds = builder.myLockUpdateTimeInSeconds;
        myFailedLockRetryAttempts = builder.myFailedLockRetryAttempts;
    }

    public LockCache getLockCache()
    {
        return myLockCache;
    }

    public long getLockUpdateTimeInSeconds()
    {
        return myLockUpdateTimeInSeconds;
    }

    public int getFailedLockRetryAttempts()
    {
        return myFailedLockRetryAttempts;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private LockCache myLockCache;
        private int myLockUpdateTimeInSeconds;
        private int myFailedLockRetryAttempts;

        public final Builder withLockUpdateTimeInSeconds(final int lockTimeInSeconds)
        {
            myLockUpdateTimeInSeconds = lockTimeInSeconds;
            return this;
        }

        public final Builder withFailedLockRetryAttempts(final int failedLockRetryAttempts)
        {
            myFailedLockRetryAttempts = failedLockRetryAttempts;
            return this;
        }

        public final Builder withLockCache(final LockCache lockCache)
        {
            myLockCache = lockCache;
            return this;
        }

        public final CASLockFactoryCacheContext build()
        {
            return new CASLockFactoryCacheContext(this);
        }
    }
}
