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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class Alarm
{
    private static final int DEFAULT_EIGHT_DAYS = 8;
    private static final int DEFAULT_TEN_DAYS = 10;

    private Class<? extends RepairFaultReporter> myFaultReporterClass = LoggingFaultReporter.class;
    private Interval myWarningInterval = new Interval(DEFAULT_EIGHT_DAYS, TimeUnit.DAYS);
    private Interval myErrorInterval = new Interval(DEFAULT_TEN_DAYS, TimeUnit.DAYS);

    public Alarm()
    {
        // Default constructor for jackson
    }

    public Alarm(final Interval warningInterval, final Interval errorInterval)
    {
        myWarningInterval = warningInterval;
        myErrorInterval = errorInterval;
    }

    @JsonProperty("faultReporter")
    public final Class<? extends RepairFaultReporter> getFaultReporterClass()
    {
        return myFaultReporterClass;
    }

    @JsonProperty("faultReporter")
    public final void setFaultReporterClass(final Class<? extends RepairFaultReporter> faultReporterClass)
    {
        myFaultReporterClass = faultReporterClass;
    }

    @JsonProperty("warn")
    public final Interval getWarningInverval()
    {
        return myWarningInterval;
    }

    @JsonProperty("warn")
    public final void setWarningInterval(final Interval warningInterval)
    {
        myWarningInterval = warningInterval;
    }

    @JsonProperty("error")
    public final Interval getErrorInterval()
    {
        return myErrorInterval;
    }

    @JsonProperty("error")
    public final void setErrorInterval(final Interval errorInterval)
    {
        myErrorInterval = errorInterval;
    }
}
