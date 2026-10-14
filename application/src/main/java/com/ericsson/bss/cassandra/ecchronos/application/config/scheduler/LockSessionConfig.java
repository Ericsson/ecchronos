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

import java.util.Locale;
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
 * <p>
 * <strong>Backward compatibility:</strong> earlier versions declared {@code scheduler.session_window} as a bare
 * interval, i.e. {@code session_window: {time: N, unit: X}}. To avoid silently reverting such a configuration to
 * the default window, this type also accepts {@code time} and {@code unit} directly at the {@code session_window}
 * level (the legacy form) as well as the new nested {@code interval: {time, unit}} form. If both are present the
 * nested {@code interval} wins. {@code enabled} defaults to {@code true}, preserving the historical behaviour for
 * old configuration files that do not set it.
 */
public class LockSessionConfig
{
    private static final int DEFAULT_SESSION_WINDOW_SECONDS = 300;
    private boolean myEnabled = true;
    private Interval mySessionWindow = new Interval(DEFAULT_SESSION_WINDOW_SECONDS, TimeUnit.SECONDS);
    private boolean myForceRefresh = false;
    // Tracks whether the new nested "interval" form was supplied, so the legacy flat "time"/"unit" form does not
    // override it when both happen to be present.
    private boolean mySessionWindowSetFromInterval = false;

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
     * Whether force-refresh is enabled. When {@code true}, a repair session that drains a job's current task
     * snapshot within the window immediately forces a repair-state refresh and continues the job in the same
     * session, instead of waiting for the throttled refresh; this reduces scheduler idle time between bursts.
     * Defaults to {@code false} to preserve the historical behaviour.
     *
     * @return {@code true} if force-refresh is enabled.
     */
    @JsonProperty("force_refresh")
    public boolean isForceRefresh()
    {
        return myForceRefresh;
    }

    /**
     * Set whether force-refresh is enabled.
     *
     * @param forceRefresh {@code true} to force a repair-state refresh and re-drive a job within the same session
     *                     once its current snapshot is drained; {@code false} for the historical behaviour.
     */
    @JsonProperty("force_refresh")
    public void setForceRefresh(final boolean forceRefresh)
    {
        myForceRefresh = forceRefresh;
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
     * Sets the session window interval that limits how long a repair session can run (new nested form).
     *
     * @param sessionWindow the session window interval
     */
    @JsonProperty("interval")
    public final void setSessionWindow(final Interval sessionWindow)
    {
        if (sessionWindow != null)
        {
            mySessionWindow = sessionWindow;
            mySessionWindowSetFromInterval = true;
        }
    }

    /**
     * Legacy accessor for the session-window time, kept so the old flat form
     * ({@code session_window: {time, unit}}) deserializes into the window instead of being ignored.
     *
     * @return the session window time value in its configured unit.
     */
    @JsonProperty("time")
    public final long getTime()
    {
        return mySessionWindow.getTime();
    }

    /**
     * Legacy setter for the old flat {@code session_window.time}. Only applied when the new nested
     * {@code interval} form was not provided.
     *
     * @param time the session window time value.
     */
    @JsonProperty("time")
    public final void setTime(final long time)
    {
        if (!mySessionWindowSetFromInterval)
        {
            mySessionWindow = new Interval(time, mySessionWindow.getUnit());
        }
    }

    /**
     * Legacy accessor for the session-window unit (see {@link #getTime()}).
     *
     * @return the session window time unit.
     */
    @JsonProperty("unit")
    public final TimeUnit getUnit()
    {
        return mySessionWindow.getUnit();
    }

    /**
     * Legacy setter for the old flat {@code session_window.unit}. Only applied when the new nested
     * {@code interval} form was not provided.
     *
     * @param unit the session window time unit name (for example {@code MINUTES}).
     */
    @JsonProperty("unit")
    public final void setUnit(final String unit)
    {
        if (!mySessionWindowSetFromInterval)
        {
            mySessionWindow = new Interval(mySessionWindow.getTime(),
                    TimeUnit.valueOf(unit.toUpperCase(Locale.US)));
        }
    }
}
