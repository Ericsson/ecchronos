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

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.utils.converter.UnitConverter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairConfig.class);

    private static final int DAYS_INTERVAL = 7;
    private static final int BACKOFF_MINUTES = 30;
    private static final int DAYS_INITIAL_DELAY = 1;

    private Interval myRepairInterval = new Interval(DAYS_INTERVAL, TimeUnit.DAYS);
    private double myUnwindRatio = 0.0d;
    private long mySizeTarget = RepairConfiguration.FULL_REPAIR_SIZE;
    private Interval myBackoff = new Interval(BACKOFF_MINUTES, TimeUnit.MINUTES);
    private boolean myIgnoreTwcsTables = false;
    private RepairType myRepairType = RepairType.VNODE;

    private Priority myPriority = new Priority();

    private Interval myInitialDelay = new Interval(DAYS_INITIAL_DELAY, TimeUnit.DAYS);

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
    public final Interval getRepairInterval()
    {
        return myRepairInterval;
    }

    @JsonProperty("interval")
    public final void setRepairInterval(final Interval repairInterval)
    {
        myRepairInterval = repairInterval;
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
    public final RepairType getRepairType()
    {
        return myRepairType;
    }

    @JsonProperty("repair_type")
    public final void setRepairType(final String repairType)
    {
        myRepairType = RepairType.valueOf(repairType.toUpperCase(Locale.US));
    }

    public final void validate(final String repairConfigType)
    {
        long repairIntervalSeconds = myRepairInterval.getInterval(TimeUnit.SECONDS);
        long initialDelaySeconds = myInitialDelay.getInterval(TimeUnit.SECONDS);

        if (repairIntervalSeconds < initialDelaySeconds)
        {
            LOG.warn("{} repair interval ({}s) is shorter than initial delay ({}s). Will use {}s as initial delay.",
                    repairConfigType, repairIntervalSeconds, initialDelaySeconds, repairIntervalSeconds);
            myInitialDelay = new Interval(myRepairInterval.getTime(), myRepairInterval.getUnit());
        }
    }

    @JsonProperty("initial_delay")
    public final Interval getInitialDelay()
    {
        return myInitialDelay;
    }

    @JsonProperty("initial_delay")
    public final void setInitialDelay(final Interval initialDelay)
    {
        myInitialDelay = initialDelay;
    }

    /**
     * Convert this object to {@link RepairConfiguration}.
     * @return {@link RepairConfiguration}
     */
    public RepairConfiguration asRepairConfiguration()
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(myRepairInterval.getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withIgnoreTWCSTables(myIgnoreTwcsTables)
                .withRepairUnwindRatio(myUnwindRatio)
                .withTargetRepairSizeInBytes(mySizeTarget)
                .withBackoff(myBackoff.getInterval(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .withRepairType(myRepairType)
                .withPriorityGranularityUnit(myPriority.getPriorityGranularityUnit())
                .withInitialDelay(myInitialDelay.getInterval(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }
}

