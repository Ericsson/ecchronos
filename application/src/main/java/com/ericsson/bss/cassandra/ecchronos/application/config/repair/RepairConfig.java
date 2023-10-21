/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.config.Interval;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RepairConfig
{
    private static final int DAYS_INTERVAL = 7;
    private static final int DAYS_WARNING = 8;
    private static final int DAYS_ERROR = 10;
    private static final int BACKOFF_MINUTES = 30;

    private Interval myRepairInterval = new Interval(DAYS_INTERVAL, TimeUnit.DAYS);
    private Alarm myAlarm = new Alarm(new Interval(DAYS_WARNING, TimeUnit.DAYS),
            new Interval(DAYS_ERROR, TimeUnit.DAYS));
    private double myUnwindRatio = 0.0d;
    private long mySizeTarget = RepairConfiguration.FULL_REPAIR_SIZE;
    private Interval myBackoff = new Interval(BACKOFF_MINUTES, TimeUnit.MINUTES);
    private boolean myIgnoreTwcsTables = false;
    private RepairOptions.RepairType myRepairType = RepairOptions.RepairType.VNODE;

    private Priority myPriority = new Priority();

    public final Priority getPriority()
    {
        return  myPriority;
    }
    @JsonProperty("priority")
    public final void setPriority(final Priority priority)
    {
        myPriority = priority;
    }

    @JsonProperty("interval")
    public final void setRepairInterval(final Interval repairInterval)
    {
        myRepairInterval = repairInterval;
    }

    @JsonProperty("alarm")
    public final Alarm getAlarm()
    {
        return myAlarm;
    }

    @JsonProperty("alarm")
    public final void setAlarm(final Alarm alarm)
    {
        myAlarm = alarm;
    }

    @JsonProperty("unwind_ratio")
    public final void setUnwindRatio(final double unwindRatio)
    {
        myUnwindRatio = unwindRatio;
    }

    @JsonProperty("size_target")
    public final void setSizeTarget(final String sizeTarget)
    {
        if (sizeTarget == null)
        {
            mySizeTarget = RepairConfiguration.FULL_REPAIR_SIZE;
        }
        else
        {
            mySizeTarget = UnitConverter.toBytes(sizeTarget);
        }
    }

    @JsonProperty("backoff")
    public final Interval getBackoff()
    {
        return myBackoff;
    }

    @JsonProperty("backoff")
    public final void setBackoff(final Interval backoff)
    {
        myBackoff = backoff;
    }

    @JsonProperty("ignore_twcs_tables")
    public final boolean getIgnoreTWCSTables()
    {
        return myIgnoreTwcsTables;
    }

    @JsonProperty("ignore_twcs_tables")
    public final void setIgnoreTwcsTables(final boolean ignoreTWCSTables)
    {
        myIgnoreTwcsTables = ignoreTWCSTables;
    }

    @JsonProperty("repair_type")
    public final RepairOptions.RepairType getRepairType()
    {
        return myRepairType;
    }

    @JsonProperty("repair_type")
    public final void setRepairType(final String repairType)
    {
        myRepairType = RepairOptions.RepairType.valueOf(repairType.toUpperCase(Locale.US));
    }

    public final void validate(final String repairConfigType)
    {
        long repairIntervalSeconds = myRepairInterval.getInterval(TimeUnit.SECONDS);
        long warningIntervalSeconds = myAlarm.getWarningInverval().getInterval(TimeUnit.SECONDS);
        if (repairIntervalSeconds >= warningIntervalSeconds)
        {
            throw new IllegalArgumentException(String.format("%s repair interval must be shorter than warning interval."
                    + " Current repair interval: %d seconds, warning interval: %d seconds", repairConfigType,
                    repairIntervalSeconds, warningIntervalSeconds));
        }
        long errorIntervalSeconds = myAlarm.getErrorInterval().getInterval(TimeUnit.SECONDS);
        if (warningIntervalSeconds >= errorIntervalSeconds)
        {
            throw new IllegalArgumentException(String.format("%s warning interval must be shorter than error interval."
                    + " Current warning interval: %d seconds, error interval: %d seconds", repairConfigType,
                    warningIntervalSeconds, errorIntervalSeconds));
        }
    }

    /**
     * Convert this object to {@link com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration}.
     * @return {@link com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration}
     */
    public RepairConfiguration asRepairConfiguration()
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(myRepairInterval.getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairWarningTime(myAlarm.getWarningInverval().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairErrorTime(myAlarm.getErrorInterval().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withIgnoreTWCSTables(myIgnoreTwcsTables)
                .withRepairUnwindRatio(myUnwindRatio)
                .withTargetRepairSizeInBytes(mySizeTarget)
                .withBackoff(myBackoff.getInterval(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRepairType(myRepairType)
                .withPriorityGranularityUnit(myPriority.getPriorityGranularityUnit())
                .build();
    }
}
