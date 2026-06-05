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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration for table repair scheduling parameters. */
public class RepairConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairConfig.class);

    private static final int DAYS_INTERVAL = 7;
    private static final int DAYS_WARNING = 8;
    private static final int DAYS_ERROR = 10;
    private static final int BACKOFF_MINUTES = 30;
    private static final int DAYS_INITIAL_DELAY = 1;

    private Interval myRepairInterval = new Interval(DAYS_INTERVAL, TimeUnit.DAYS);
    private Alarm myAlarm = new Alarm(new Interval(DAYS_WARNING, TimeUnit.DAYS),
            new Interval(DAYS_ERROR, TimeUnit.DAYS));
    private double myUnwindRatio = 0.0d;
    private long mySizeTarget = RepairConfiguration.FULL_REPAIR_SIZE;
    private Interval myBackoff = new Interval(BACKOFF_MINUTES, TimeUnit.MINUTES);
    private boolean myIgnoreTwcsTables = false;
    private RepairOptions.RepairType myRepairType = RepairOptions.RepairType.VNODE;

    private Priority myPriority = new Priority();

    private Interval myInitialDelay = new Interval(DAYS_INITIAL_DELAY, TimeUnit.DAYS);

    /** Default constructor. */
    public RepairConfig()
    {
    }

    /**
     * Returns the priority.
     * @return the priority
     */
    public final Priority getPriority()
    {
        return  myPriority;
    }

    /**
     * Sets the priority.
     * @param priority the job priority value
     */
    @JsonProperty("priority")
    public final void setPriority(final Priority priority)
    {
        myPriority = priority;
    }

    /**
     * Returns the repair interval.
     * @return the repair interval
     */
    @JsonProperty("interval")
    public final Interval getRepairInterval()
    {
        return myRepairInterval;
    }

    /**
     * Sets the repair interval.
     * @param repairInterval the repair interval
     */
    @JsonProperty("interval")
    public final void setRepairInterval(final Interval repairInterval)
    {
        myRepairInterval = repairInterval;
    }

    /**
     * Returns the alarm.
     * @return the alarm
     */
    @JsonProperty("alarm")
    public final Alarm getAlarm()
    {
        return myAlarm;
    }

    /**
     * Sets the alarm.
     * @param alarm the alarm configuration
     */
    @JsonProperty("alarm")
    public final void setAlarm(final Alarm alarm)
    {
        myAlarm = alarm;
    }

    /**
     * Sets the unwind ratio.
     * @param unwindRatio the unwind ratio
     */
    @JsonProperty("unwind_ratio")
    public final void setUnwindRatio(final double unwindRatio)
    {
        myUnwindRatio = unwindRatio;
    }

    /**
     * Sets the size target.
     * @param sizeTarget the size target
     */
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

    /**
     * Returns the backoff.
     * @return the backoff
     */
    @JsonProperty("backoff")
    public final Interval getBackoff()
    {
        return myBackoff;
    }

    /**
     * Sets the backoff.
     * @param backoff the backoff delay between retries
     */
    @JsonProperty("backoff")
    public final void setBackoff(final Interval backoff)
    {
        myBackoff = backoff;
    }

    /**
     * Returns the ignore TWCS tables.
     * @return the ignore TWCS tables
     */
    @JsonProperty("ignore_twcs_tables")
    public final boolean getIgnoreTWCSTables()
    {
        return myIgnoreTwcsTables;
    }

    /**
     * Sets the ignore TWCS tables.
     * @param ignoreTWCSTables the ignore TWCS tables
     */
    @JsonProperty("ignore_twcs_tables")
    public final void setIgnoreTwcsTables(final boolean ignoreTWCSTables)
    {
        myIgnoreTwcsTables = ignoreTWCSTables;
    }

    /**
     * Returns the repair type.
     * @return the repair type
     */
    @JsonProperty("repair_type")
    public final RepairOptions.RepairType getRepairType()
    {
        return myRepairType;
    }

    /**
     * Sets the repair type.
     * @param repairType the repair type
     */
    @JsonProperty("repair_type")
    public final void setRepairType(final String repairType)
    {
        myRepairType = RepairOptions.RepairType.valueOf(repairType.toUpperCase(Locale.US));
    }

    /**
     * Validates the current state.
     * @param repairConfigType the repair config type
     */
    public final void validate(final String repairConfigType)
    {
        long repairIntervalSeconds = myRepairInterval.getInterval(TimeUnit.SECONDS);
        long warningIntervalSeconds = myAlarm.getWarningInverval().getInterval(TimeUnit.SECONDS);
        long initialDelaySeconds = myInitialDelay.getInterval(TimeUnit.SECONDS);
        if (repairIntervalSeconds >= warningIntervalSeconds)
        {
            throw new IllegalArgumentException(String.format("""
                    %s repair interval must be shorter than warning interval.\
                     Current repair interval: %d seconds, warning interval: %d seconds\
                    """,
                    repairConfigType,
                    repairIntervalSeconds,
                    warningIntervalSeconds));
        }

        if (repairIntervalSeconds < initialDelaySeconds)
        {
            LOG.warn("{} repair interval ({}s) is shorter than initial delay ({}s). Will use {}s as initial delay.",
                    repairConfigType,
                    repairIntervalSeconds,
                    initialDelaySeconds,
                    repairIntervalSeconds);
            myInitialDelay = new Interval(myRepairInterval.getTime(), myRepairInterval.getUnit());
        }

        long errorIntervalSeconds = myAlarm.getErrorInterval().getInterval(TimeUnit.SECONDS);
        if (warningIntervalSeconds >= errorIntervalSeconds)
        {
            throw new IllegalArgumentException(String.format("""
                    %s warning interval must be shorter than error interval.\
                     Current warning interval: %d seconds, error interval: %d seconds\
                    """,
                    repairConfigType,
                    warningIntervalSeconds,
                    errorIntervalSeconds));
        }
    }

    /**
     * Returns the initial delay.
     * @return the initial delay
     */
    @JsonProperty("initial_delay")
    public final Interval getInitialDelay()
    {
        return myInitialDelay;
    }

    /**
     * Sets the initial delay.
     * @param initialDelay the initial delay
     */
    @JsonProperty("initial_delay")
    public final void setInitialDelay(final Interval initialDelay)
    {
        myInitialDelay = initialDelay;
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
                .withInitialDelay(myInitialDelay.getInterval(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build();
    }
}
