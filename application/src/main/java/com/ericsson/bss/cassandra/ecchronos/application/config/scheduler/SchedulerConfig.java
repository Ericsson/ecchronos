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
package com.ericsson.bss.cassandra.ecchronos.application.config.scheduler;

import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

/** Configuration for the job scheduler. */
public class SchedulerConfig
{
    private static final int THIRTY_SECONDS = 30;
    private static final int DEFAULT_COOLDOWN_SECONDS = 0;

    private Interval myFrequency = new Interval(THIRTY_SECONDS, TimeUnit.SECONDS);
    private Interval myCooldown = new Interval(DEFAULT_COOLDOWN_SECONDS, TimeUnit.SECONDS);
    private int myMaxConcurrency = 0;
    private LockSessionConfig myLockSession = new LockSessionConfig();

    /** Default constructor. */
    public SchedulerConfig()
    {
    }

    /**
     * Returns the frequency.
     * @return the frequency
     */
    @JsonProperty("frequency")
    public final Interval getFrequency()
    {
        return myFrequency;
    }

    /**
     * Sets the frequency.
     * @param frequency the reporting frequency
     */
    @JsonProperty("frequency")
    public final void setFrequency(final Interval frequency)
    {
        myFrequency = frequency;
    }

    /**
     * Returns the cooldown interval between repair sessions.
     *
     * @return the cooldown interval
     */
    @JsonProperty("cooldown")
    public final Interval getCooldown()
    {
        return myCooldown;
    }

    /**
     * Sets the cooldown interval between repair sessions.
     *
     * @param cooldown the cooldown interval
     */
    @JsonProperty("cooldown")
    public final void setCooldown(final Interval cooldown)
    {
        myCooldown = cooldown;
    }

    /**
     * Returns the maximum number of concurrent CPU-bound scheduler threads. A value of {@code 0} (or negative)
     * means no cap: the scheduler uses one thread per managed node. Setting a positive value bounds the concurrency
     * regardless of node count, which is useful on CPU-constrained deployments (see #1850).
     *
     * @return the maximum concurrency, or {@code 0} for unbounded.
     */
    @JsonProperty("max_concurrency")
    public final int getMaxConcurrency()
    {
        return myMaxConcurrency;
    }

    /**
     * Sets the maximum number of concurrent CPU-bound scheduler threads. Use {@code 0} for no cap (one thread per
     * managed node).
     *
     * @param maxConcurrency the maximum concurrency, or {@code 0} for unbounded.
     */
    @JsonProperty("max_concurrency")
    public final void setMaxConcurrency(final int maxConcurrency)
    {
        myMaxConcurrency = maxConcurrency;
    }

    /**
     * Returns the lock-session configuration controlling whether distributed locks are batched for the duration of
     * a session (default) or acquired and released per task (sidecar semantics).
     *
     * @return the lock-session configuration.
     */
    @JsonProperty("session_window")
    public final LockSessionConfig getLockSession()
    {
        return myLockSession;
    }

    /**
     * Sets the lock-session configuration.
     *
     * @param lockSession the lock-session configuration; ignored when {@code null}.
     */
    @JsonProperty("session_window")
    public final void setLockSession(final LockSessionConfig lockSession)
    {
        if (lockSession != null)
        {
            myLockSession = lockSession;
        }
    }
}
