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

public class Interval
{
    static final int DEFAULT_TIME_IN_MINUTES = 60;
    private long myTime = DEFAULT_TIME_IN_MINUTES;
    private TimeUnit myUnit = TimeUnit.MINUTES;

    public Interval()
    {
        // Default constructor for jackson
    }

    public Interval(final long time, final TimeUnit timeUnit)
    {
        myTime = time;
        myUnit = timeUnit;
    }

    public final long getInterval(final TimeUnit timeUnit)
    {
        return timeUnit.convert(myTime, myUnit);
    }

    @JsonProperty("time")
    public final long getTime()
    {
        return myTime;
    }

    @JsonProperty("time")
    public final void setTime(final long time)
    {
        myTime = time;
    }

    @JsonProperty("unit")
    public final TimeUnit getUnit()
    {
        return myUnit;
    }

    @JsonProperty("unit")
    public final void setUnit(final String unit)
    {
        myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
    }
}
