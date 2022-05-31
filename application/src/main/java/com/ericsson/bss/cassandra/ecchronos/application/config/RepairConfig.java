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

public class RepairConfig
{
    private Config.Interval interval = new Config.Interval(7, TimeUnit.DAYS);
    private Config.Alarm alarm = new Config.Alarm(new Config.Interval(8, TimeUnit.DAYS),
            new Config.Interval(10, TimeUnit.DAYS));
    private double unwind_ratio = 0.0d;
    private long size_target = RepairConfiguration.FULL_REPAIR_SIZE;

    public Config.Alarm getAlarm()
    {
        return alarm;
    }

    public void setInterval(Config.Interval interval)
    {
        this.interval = interval;
    }

    public void setAlarm(Config.Alarm alarm)
    {
        this.alarm = alarm;
    }

    public void setUnwind_ratio(double unwind_ratio)
    {
        this.unwind_ratio = unwind_ratio;
    }

    public void setSize_target(String size_target)
    {
        if (size_target == null)
        {
            this.size_target = RepairConfiguration.FULL_REPAIR_SIZE;
        }
        else
        {
            this.size_target = UnitConverter.toBytes(size_target);
        }
    }

    public RepairConfiguration asRepairConfiguration()
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(interval.getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairWarningTime(alarm.getWarn().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairErrorTime(alarm.getError().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairUnwindRatio(unwind_ratio)
                .withTargetRepairSizeInBytes(size_target)
                .build();
    }
}
