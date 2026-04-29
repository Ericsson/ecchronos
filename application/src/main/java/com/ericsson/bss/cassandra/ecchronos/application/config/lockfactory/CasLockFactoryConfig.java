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
package com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory;

import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Locale;

public class CasLockFactoryConfig
{
    private static final long DEFAULT_EXPIRY_TIME_IN_SECONDS = 30L;
    private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";
    private static final int MIN_LOCKS_PER_RESOURCE = 1;
    private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
    private long myExpiryTimeInSeconds = DEFAULT_EXPIRY_TIME_IN_SECONDS;
    private ConsistencyType myConsistencySerial = ConsistencyType.SERIAL;
    private int myLocksPerResource = MIN_LOCKS_PER_RESOURCE;

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

    @JsonProperty ("consistencySerial")
    public final ConsistencyType getConsistencySerial()
    {
        return myConsistencySerial;
    }

    @JsonProperty ("consistencySerial")
    public final void setConsistencySerial(final String consistencySerial)
    {
        myConsistencySerial = ConsistencyType.valueOf(consistencySerial.toUpperCase(Locale.US));
    }

    public final int getLocksPerResource()
    {
        return myLocksPerResource;
    }

    @JsonProperty ("locks_per_resource")
    public final void setLocksPerResource(final int locksPerResource)
    {
        if (locksPerResource < MIN_LOCKS_PER_RESOURCE)
        {
            throw new IllegalArgumentException("locks_per_resource must be at least 1");
        }
        myLocksPerResource = locksPerResource;
    }
}
