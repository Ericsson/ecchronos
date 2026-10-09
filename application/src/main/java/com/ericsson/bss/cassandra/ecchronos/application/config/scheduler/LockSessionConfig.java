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
package com.ericsson.bss.cassandra.ecchronos.application.config.scheduler;

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the repair lock session (batched locking).
 * <p>
 * When {@code enabled} is {@code true} (the default) a repair session acquires the distributed locks for the
 * replica resources it touches and holds them for the duration of the {@code session_window}, reusing shared
 * resource locks across tasks. This reduces lock-acquisition overhead but holds replica-set locks for the whole
 * window, which can serialize other nodes managed by the same standalone instance.
 * <p>
 * When {@code enabled} is {@code false} the scheduler acquires and releases the distributed lock <em>per task</em>
 * (sidecar semantics): a replica-set lock is held only for the duration of a single repair task, maximising
 * cross-node concurrency at the cost of more frequent lock acquisitions.
 */
public class LockSessionConfig
{
    private static final int DEFAULT_SESSION_WINDOW_SECONDS = 300;
    private boolean myEnabled = true;
    private Interval mySessionWindow = new Interval(DEFAULT_SESSION_WINDOW_SECONDS, TimeUnit.SECONDS);

    /** Default constructor. */
    public LockSessionConfig()
    {
    }

    /**
     * Whether batched lock sessions are enabled.
     *
     * @return {@code true} if locks are held for the whole session window (batched); {@code false} if locks are
     *         acquired and released per task (sidecar semantics).
     */
    @JsonProperty("enabled")
    public boolean isEnabled()
    {
        return myEnabled;
    }

    /**
     * Set whether batched lock sessions are enabled.
     *
     * @param enabled {@code true} to hold locks for the whole session window (batched); {@code false} to acquire
     *                and release locks per task (sidecar semantics).
     */
    @JsonProperty("enabled")
    public void setEnabled(final boolean enabled)
    {
        myEnabled = enabled;
    }

    /**
     * Returns the session window interval that limits how long a repair session can run.
     *
     * @return the session window interval
     */
    @JsonProperty("interval")
    public final Interval getSessionWindow()
    {
        return mySessionWindow;
    }

    /**
     * Sets the session window interval that limits how long a repair session can run.
     *
     * @param sessionWindow the session window interval
     */
    @JsonProperty("interval")
    public final void setSessionWindow(final Interval sessionWindow)
    {
        mySessionWindow = sessionWindow;
    }
}
