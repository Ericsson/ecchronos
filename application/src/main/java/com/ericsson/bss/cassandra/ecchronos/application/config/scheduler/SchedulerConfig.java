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
    private static final int DEFAULT_SESSION_WINDOW_SECONDS = 300;
    private static final int DEFAULT_COOLDOWN_SECONDS = 0;

    private Interval myFrequency = new Interval(THIRTY_SECONDS, TimeUnit.SECONDS);
    private Interval mySessionWindow = new Interval(DEFAULT_SESSION_WINDOW_SECONDS, TimeUnit.SECONDS);
    private Interval myCooldown = new Interval(DEFAULT_COOLDOWN_SECONDS, TimeUnit.SECONDS);

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
     * Returns the session window interval that limits how long a repair session can run.
     *
     * @return the session window interval
     */
    @JsonProperty("session_window")
    public final Interval getSessionWindow()
    {
        return mySessionWindow;
    }

    /**
     * Sets the session window interval that limits how long a repair session can run.
     *
     * @param sessionWindow the session window interval
     */
    @JsonProperty("session_window")
    public final void setSessionWindow(final Interval sessionWindow)
    {
        mySessionWindow = sessionWindow;
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
}
