/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for retrying on-demand repair tasks that fail transiently (for example on a JMX request failure).
 * See issue #1848. A single on-demand repair task may be retried, up to {@code attempts} times, with a
 * {@code backoff} delay between attempts. {@code attempts = 1} disables retry (the previous behaviour).
 */
public class OnDemandRetryConfig
{
    private static final int DEFAULT_ATTEMPTS = 3;
    private static final int DEFAULT_BACKOFF_MINUTES = 1;

    private int myAttempts = DEFAULT_ATTEMPTS;
    private Interval myBackoff = new Interval(DEFAULT_BACKOFF_MINUTES, TimeUnit.MINUTES);

    /** Default constructor. */
    public OnDemandRetryConfig()
    {
        // Default constructor for Jackson
    }

    /**
     * The maximum number of attempts per on-demand repair task (1 = no retry).
     *
     * @return the attempts.
     */
    @JsonProperty("attempts")
    public int getAttempts()
    {
        return myAttempts;
    }

    /**
     * Sets the maximum number of attempts per on-demand repair task.
     *
     * @param attempts the attempts (1 = no retry).
     */
    @JsonProperty("attempts")
    public void setAttempts(final int attempts)
    {
        myAttempts = attempts;
    }

    /**
     * The delay between retry attempts.
     *
     * @return the backoff interval.
     */
    @JsonProperty("backoff")
    public Interval getBackoff()
    {
        return myBackoff;
    }

    /**
     * Sets the delay between retry attempts.
     *
     * @param backoff the backoff interval.
     */
    @JsonProperty("backoff")
    public void setBackoff(final Interval backoff)
    {
        myBackoff = backoff;
    }

    /**
     * The backoff delay in milliseconds.
     *
     * @return the backoff in milliseconds.
     */
    public long getBackoffInMs()
    {
        return myBackoff.getInterval(TimeUnit.MILLISECONDS);
    }
}
