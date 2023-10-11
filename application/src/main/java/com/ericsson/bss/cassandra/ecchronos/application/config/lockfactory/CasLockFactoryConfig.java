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
package com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CasLockFactoryConfig
{
    private static final long DEFAULT_LOCK_TIME_IN_SECONDS = 600L;
    private static final long DEFAULT_LOCK_UPDATE_TIME_IN_SECONDS = 60L;
    private static final long DEFAULT_EXPIRY_TIME_IN_SECONDS = 30L;
    private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";
    private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
    private long myLockTimeInSeconds = DEFAULT_LOCK_TIME_IN_SECONDS;
    private long myLockUpdateTimeInSeconds = DEFAULT_LOCK_UPDATE_TIME_IN_SECONDS;
    private long myExpiryTimeInSeconds = DEFAULT_EXPIRY_TIME_IN_SECONDS;

    public final long getLockTimeInSeconds()
    {
        return myLockTimeInSeconds;
    }

    @JsonProperty ("lock_time_in_seconds")
    public final void setLockTimeInSeconds(final long lockTimeInSeconds)
    {
        myLockTimeInSeconds = lockTimeInSeconds;
    }

    public final long getLockUpdateTimeInSeconds()
    {
        return myLockUpdateTimeInSeconds;
    }

    @JsonProperty ("lock_update_time_in_seconds")
    public final void setLockUpdateTimeInSeconds(final long lockUpdateTimeInSeconds)
    {
        myLockUpdateTimeInSeconds = lockUpdateTimeInSeconds;
    }

    public final long getFailureCacheExpiryTimeInSeconds()
    {
        return myExpiryTimeInSeconds;
    }

    @JsonProperty ("cache_expiry_time_in_seconds")
    public final void setFailureCacheExpiryTimeInSeconds(final long expiryTimeInSeconds)
    {
        myExpiryTimeInSeconds = expiryTimeInSeconds;
    }

    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    @JsonProperty ("keyspace")
    public final void setKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
    }
}
