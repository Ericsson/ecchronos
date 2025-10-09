/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CQLRetryPolicyConfig
{
    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final int INITIAL_BACKOFF_INTERVAL_IN_MS = 5000; // 5 seconds
    private static final int MAX_BACKOFF_INTERVAL_IN_MS = 30000; // 30 seconds
    private static final int DISABLE_MAX_DELAY = 0;

    private Integer myMaxAttempts = DEFAULT_MAX_ATTEMPTS;
    private TimeUnit myUnit = TimeUnit.SECONDS;
    private long myDelay = INITIAL_BACKOFF_INTERVAL_IN_MS;
    private long myMaxDelay = MAX_BACKOFF_INTERVAL_IN_MS;

    @JsonCreator
    public CQLRetryPolicyConfig(@JsonProperty("unit") final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }

    public CQLRetryPolicyConfig()
    {
    }

    public final long currentDelay(final Integer attempt)
    {
        long currentDelay = (long) (myDelay * Math.pow(2, attempt));
        if ((myMaxDelay > DISABLE_MAX_DELAY) && (currentDelay > myMaxDelay))
        {
            currentDelay = myMaxDelay;
        }
        return currentDelay;
    }

    public final Integer getMaxAttempts()
    {
        return myMaxAttempts;
    }

    public final long getMaxDelay()
    {
        return myMaxDelay;
    }

    public final TimeUnit getUnit()
    {
        return myUnit;
    }

    public final long getDelay()
    {
        return myDelay;
    }

    @JsonProperty("maxAttempts")
    public final void setMaxAttempts(final Integer maxAttempts)
    {
        myMaxAttempts = maxAttempts;
    }

    @JsonProperty("delay")
    public final void setDelay(final Integer delay)
    {
        myDelay = myUnit.toMillis(delay);
    }

    @JsonProperty("maxDelay")
    public final void setMaxDelay(final Integer maxDelay)
    {
        myMaxDelay = myUnit.toMillis(maxDelay);
    }

    @VisibleForTesting
    public final void setUnit(final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }
}

