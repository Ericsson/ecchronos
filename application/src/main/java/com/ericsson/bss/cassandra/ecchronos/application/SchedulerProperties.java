/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class SchedulerProperties
{
    private static final String CONFIG_RUN_INTERVAL = "scheduler.run.interval.time";
    private static final String CONFIG_RUN_INTERVAL_TIMEUNIT = "scheduler.run.interval.time.unit";
    private static final String DEFAULT_RUN_INTERVAL = "30";
    private static final String DEFAULT_RUN_INTERVAL_TIMEUNIT = TimeUnit.SECONDS.toString();

    private final long myRunInterval;
    private final TimeUnit myTimeUnit;

    private SchedulerProperties(long runInterval, TimeUnit timeUnit)
    {
        myRunInterval = runInterval;
        myTimeUnit = timeUnit;
    }

    public long getRunInterval()
    {
        return myRunInterval;
    }

    public TimeUnit getTimeUnit()
    {
        return myTimeUnit;
    }

    @Override
    public String toString()
    {
        return String.format("(schedulerRunInterval=%s, schedulerRunIntervalTimeUnit=%s)", myRunInterval, myTimeUnit);
    }

    public static SchedulerProperties from(Properties properties) throws ConfigurationException
    {
        String durationProperty = CONFIG_RUN_INTERVAL;
        String timeUnitProperty = CONFIG_RUN_INTERVAL_TIMEUNIT;

        if (properties.containsKey(durationProperty) != properties.containsKey(timeUnitProperty))
        {
            throw new ConfigurationException("Both TimeUnit and duration of needs to be set or none of them(" + durationProperty + " & " + timeUnitProperty + ")");
        }

        TimeUnit intervalTimeUnit;

        try
        {
            intervalTimeUnit = TimeUnit.valueOf(properties.getProperty(timeUnitProperty, DEFAULT_RUN_INTERVAL_TIMEUNIT).toUpperCase(Locale.US));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown time unit configured for " + timeUnitProperty, e);
        }

        long interval = Long.parseLong(properties.getProperty(durationProperty, DEFAULT_RUN_INTERVAL));

        return new SchedulerProperties(interval, intervalTimeUnit);
    }
}
