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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Represents a time interval with a numeric value and a {@link TimeUnit}.
 * Used for configuring durations in repair and scheduling configurations.
 */
public class Interval
{
    static final int DEFAULT_TIME_IN_MINUTES = 60;
    private long myTime = DEFAULT_TIME_IN_MINUTES;
    private TimeUnit myUnit = TimeUnit.MINUTES;

    /** Default constructor for Jackson deserialization. */
    public Interval()
    {
        // Default constructor for jackson
    }

    /**
     * Constructs an interval with the specified time and unit.
     *
     * @param time the numeric time value
     * @param timeUnit the time unit for the interval
     */
    public Interval(final long time, final TimeUnit timeUnit)
    {
        myTime = time;
        myUnit = timeUnit;
    }

    /**
     * Returns the interval duration converted to the specified time unit.
     *
     * @param timeUnit the target time unit for conversion
     * @return the interval duration in the specified time unit
     */
    public final long getInterval(final TimeUnit timeUnit)
    {
        return timeUnit.convert(myTime, myUnit);
    }

    /**
     * Returns the numeric time value of this interval.
     *
     * @return the time value
     */
    @JsonProperty("time")
    public final long getTime()
    {
        return myTime;
    }

    /**
     * Sets the numeric time value of this interval.
     *
     * @param time the time value
     */
    @JsonProperty("time")
    public final void setTime(final long time)
    {
        myTime = time;
    }

    /**
     * Returns the time unit of this interval.
     *
     * @return the time unit
     */
    @JsonProperty("unit")
    public final TimeUnit getUnit()
    {
        return myUnit;
    }

    /**
     * Sets the time unit of this interval by parsing the given string.
     *
     * @param unit the time unit name (e.g., "SECONDS", "MINUTES", "HOURS")
     */
    @JsonProperty("unit")
    public final void setUnit(final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }
}
