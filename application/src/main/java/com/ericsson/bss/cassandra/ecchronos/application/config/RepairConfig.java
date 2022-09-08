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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;

@SuppressWarnings({"checkstyle:membername", "checkstyle:methodname"})
public class RepairConfig
{
    private static final int DAYS_INTERVAL = 7;
    private static final int DAYS_WARNING = 8;
    private static final int DAYS_ERROR = 10;

    private Config.Interval interval = new Config.Interval(DAYS_INTERVAL, TimeUnit.DAYS);
    private Config.Alarm alarm = new Config.Alarm(new Config.Interval(DAYS_WARNING, TimeUnit.DAYS),
            new Config.Interval(DAYS_ERROR, TimeUnit.DAYS));
    private double unwind_ratio = 0.0d;
    private long size_target = RepairConfiguration.FULL_REPAIR_SIZE;
    private boolean ignore_twcs_tables = false;

    public final Config.Alarm getAlarm()
    {
        return alarm;
    }

    public final void setInterval(final Config.Interval anInterval)
    {
        this.interval = anInterval;
    }

    public final void setAlarm(final Config.Alarm anAlarm)
    {
        this.alarm = anAlarm;
    }

    public final void setUnwind_ratio(final double unwindRatio)
    {
        this.unwind_ratio = unwindRatio;
    }

    public final void setSize_target(final String sizeTarget)
    {
        if (sizeTarget == null)
        {
            this.size_target = RepairConfiguration.FULL_REPAIR_SIZE;
        }
        else
        {
            this.size_target = UnitConverter.toBytes(sizeTarget);
        }
    }

    public final void setIgnore_twcs_tables(final boolean ignoreTWCSTables)
    {
        this.ignore_twcs_tables = ignoreTWCSTables;
    }

    public final boolean getIgnoreTWCSTables()
    {
        return ignore_twcs_tables;
    }

    @SuppressWarnings("checkstyle:designforextension")
    public RepairConfiguration asRepairConfiguration()
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(interval.getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairWarningTime(alarm.getWarn().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairErrorTime(alarm.getError().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withIgnoreTWCSTables(ignore_twcs_tables)
                .withRepairUnwindRatio(unwind_ratio)
                .withTargetRepairSizeInBytes(size_target)
                .build();
    }
}
