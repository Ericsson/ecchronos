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

/**
 * Configuration for the CQL retry policy with exponential backoff.
 * Defines the maximum number of retry attempts, delay intervals, and time unit
 * used when retrying failed CQL connections.
 */
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

    /**
     * Constructs a retry policy configuration with the specified time unit.
     *
     * @param unit the time unit for delay values (e.g. "SECONDS", "MILLISECONDS").
     */
    @JsonCreator
    public CQLRetryPolicyConfig(@JsonProperty("unit") final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }

    /**
     * Default constructor using default values for all retry policy settings.
     */
    public CQLRetryPolicyConfig()
    {
    }

    /**
     * Calculates the current delay for the given attempt using exponential backoff.
     * The delay is capped at the configured maximum delay if one is set.
     *
     * @param attempt the current attempt number (zero-based).
     * @return the computed delay in milliseconds.
     */
    public final long currentDelay(final Integer attempt)
    {
        long currentDelay = (long) (myDelay * Math.pow(2, attempt));
        if ((myMaxDelay > DISABLE_MAX_DELAY) && (currentDelay > myMaxDelay))
        {
            currentDelay = myMaxDelay;
        }
        return currentDelay;
    }

    /**
     * Returns the maximum number of retry attempts.
     *
     * @return the maximum number of attempts.
     */
    public final Integer getMaxAttempts()
    {
        return myMaxAttempts;
    }

    /**
     * Returns the maximum backoff delay in milliseconds.
     *
     * @return the maximum delay in milliseconds.
     */
    public final long getMaxDelay()
    {
        return myMaxDelay;
    }

    /**
     * Returns the time unit used for delay configuration.
     *
     * @return the time unit.
     */
    public final TimeUnit getUnit()
    {
        return myUnit;
    }

    /**
     * Returns the initial backoff delay in milliseconds.
     *
     * @return the initial delay in milliseconds.
     */
    public final long getDelay()
    {
        return myDelay;
    }

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param maxAttempts the maximum number of attempts.
     */
    @JsonProperty("maxAttempts")
    public final void setMaxAttempts(final Integer maxAttempts)
    {
        myMaxAttempts = maxAttempts;
    }

    /**
     * Sets the initial backoff delay, converting from the configured time unit to milliseconds.
     *
     * @param delay the delay value in the configured time unit.
     */
    @JsonProperty("delay")
    public final void setDelay(final Integer delay)
    {
        myDelay = myUnit.toMillis(delay);
    }

    /**
     * Sets the maximum backoff delay, converting from the configured time unit to milliseconds.
     *
     * @param maxDelay the maximum delay value in the configured time unit.
     */
    @JsonProperty("maxDelay")
    public final void setMaxDelay(final Integer maxDelay)
    {
        myMaxDelay = myUnit.toMillis(maxDelay);
    }

    /**
     * Sets the time unit for delay values. Intended for testing purposes.
     *
     * @param unit the time unit string (e.g. "SECONDS", "MILLISECONDS").
     */
    @VisibleForTesting
    public final void setUnit(final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }
}

