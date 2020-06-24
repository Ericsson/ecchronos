/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public final class RepairProperties
{
    private static final String CONFIG_REPAIR_INTERVAL_BASE = "repair.interval";
    private static final String CONFIG_REPAIR_PARALLELISM = "repair.parallelism";
    private static final String CONFIG_REPAIR_ALARM_WARN_BASE = "repair.alarm.warn";
    private static final String CONFIG_REPAIR_ALARM_ERROR_BASE = "repair.alarm.error";
    private static final String CONFIG_REPAIR_LOCK_TYPE = "repair.lock.type";
    private static final String CONFIG_REPAIR_UNWIND_RATIO = "repair.unwind.ratio";
    private static final String CONFIG_REPAIR_HISTORY_LOOKBACK_BASE = "repair.history.lookback";
    private static final String CONFIG_REPAIR_TARGET_REPAIR_SIZE = "repair.size.target";

    private static final String DAYS = "days";
    private static final String DEFAULT_REPAIR_INTERVAL_TIMEUNIT = DAYS;
    private static final String DEFAULT_REPAIR_INTERVAL_DURATION = "7";
    private static final String DEFAULT_REPAIR_PARALLELISM = "parallel";
    private static final String DEFAULT_REPAIR_WARN_TIMEUNIT = DAYS;
    private static final String DEFAULT_REPAIR_WARN_DURATION = "8";
    private static final String DEFAULT_REPAIR_ERROR_TIMEUNIT = DAYS;
    private static final String DEFAULT_REPAIR_ERROR_DURATION = "10";
    private static final String DEFAULT_REPAIR_LOCK_TYPE = "vnode";
    private static final String DEFAULT_REPAIR_UNWIND_RATIO = Double.toString(RepairConfiguration.NO_UNWIND);
    private static final String DEFAULT_REPAIR_HISTORY_LOOKBACK_TIMEUNIT = DAYS;
    private static final String DEFAULT_REPAIR_HISTORY_LOOKBACK_DURATION = "30";
    private static final String DEFAULT_REPAIR_TARGET_REPAIR_SIZE = "";
    private static final long DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES = RepairConfiguration.FULL_REPAIR_SIZE;

    private final long myRepairIntervalInMs;
    private final RepairOptions.RepairParallelism myRepairParallelism;
    private final long myRepairAlarmWarnInMs;
    private final long myRepairAlarmErrorInMs;
    private final RepairLockType myRepairLockType;
    private final double myRepairUnwindRatio;
    private final long myRepairHistoryLookbackInMs;
    private final long myTargetRepairSizeInBytes;

    private RepairProperties(long repairIntervalInMs, RepairOptions.RepairParallelism repairParallelism,
                             long repairAlarmWarnInMs, long repairAlarmErrorInMs, RepairLockType repairLockType,
                             double repairUnwindRatio, long repairHistoryLookbackInMs,
                             long targetRepairSizeInBytes)
    {
        myRepairIntervalInMs = repairIntervalInMs;
        myRepairParallelism = repairParallelism;
        myRepairAlarmWarnInMs = repairAlarmWarnInMs;
        myRepairAlarmErrorInMs = repairAlarmErrorInMs;
        myRepairLockType = repairLockType;
        myRepairUnwindRatio = repairUnwindRatio;
        myRepairHistoryLookbackInMs = repairHistoryLookbackInMs;
        myTargetRepairSizeInBytes = targetRepairSizeInBytes;
    }

    public long getRepairIntervalInMs()
    {
        return myRepairIntervalInMs;
    }

    public RepairOptions.RepairParallelism getRepairParallelism()
    {
        return myRepairParallelism;
    }

    public long getRepairAlarmWarnInMs()
    {
        return myRepairAlarmWarnInMs;
    }

    public long getRepairAlarmErrorInMs()
    {
        return myRepairAlarmErrorInMs;
    }

    public RepairLockType getRepairLockType()
    {
        return myRepairLockType;
    }

    public double getRepairUnwindRatio()
    {
        return myRepairUnwindRatio;
    }

    public long getRepairHistoryLookbackInMs()
    {
        return myRepairHistoryLookbackInMs;
    }

    public long getTargetRepairSizeInBytes()
    {
        return myTargetRepairSizeInBytes;
    }

    @Override
    public String toString()
    {
        return String.format("(intervalMs=%d,repairParallelism=%s,repairWarnMs=%d,repairErrorMs=%d,repairUnwindRatio=%.2f,repairHistoryLookbackInMs=%d)",
                myRepairIntervalInMs, myRepairParallelism, myRepairAlarmWarnInMs, myRepairAlarmErrorInMs, myRepairUnwindRatio, myRepairHistoryLookbackInMs);
    }

    public static RepairProperties from(Properties properties) throws ConfigurationException
    {
        long repairIntervalInMs = parseTimeUnitToMs(properties, CONFIG_REPAIR_INTERVAL_BASE,
                DEFAULT_REPAIR_INTERVAL_DURATION, DEFAULT_REPAIR_INTERVAL_TIMEUNIT);

        RepairOptions.RepairParallelism repairParallelism;

        try
        {
            repairParallelism = RepairOptions.RepairParallelism.valueOf(properties.getProperty(CONFIG_REPAIR_PARALLELISM, DEFAULT_REPAIR_PARALLELISM).toUpperCase(Locale.US));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown repair parallelism specified in '" + CONFIG_REPAIR_PARALLELISM + "'", e);
        }

        long repairAlarmWarnInMs = parseTimeUnitToMs(properties, CONFIG_REPAIR_ALARM_WARN_BASE,
                DEFAULT_REPAIR_WARN_DURATION, DEFAULT_REPAIR_WARN_TIMEUNIT);
        long repairAlarmErrorInMs = parseTimeUnitToMs(properties, CONFIG_REPAIR_ALARM_ERROR_BASE,
                DEFAULT_REPAIR_ERROR_DURATION, DEFAULT_REPAIR_ERROR_TIMEUNIT);

        RepairLockType repairLockType;

        try
        {
            repairLockType = RepairLockType.valueOf(properties.getProperty(CONFIG_REPAIR_LOCK_TYPE, DEFAULT_REPAIR_LOCK_TYPE).toUpperCase(Locale.US));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown repair lock type specified in '" + CONFIG_REPAIR_LOCK_TYPE + "'", e);
        }

        double repairUnwindRatio = Double.parseDouble(properties.getProperty(CONFIG_REPAIR_UNWIND_RATIO, DEFAULT_REPAIR_UNWIND_RATIO));

        long repairHistoryLookbackInMs = parseTimeUnitToMs(properties, CONFIG_REPAIR_HISTORY_LOOKBACK_BASE,
                DEFAULT_REPAIR_HISTORY_LOOKBACK_DURATION, DEFAULT_REPAIR_HISTORY_LOOKBACK_TIMEUNIT);

        long targetRepairSizeInBytes = DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES;

        try
        {
            String targetRepairSize = properties.getProperty(CONFIG_REPAIR_TARGET_REPAIR_SIZE, DEFAULT_REPAIR_TARGET_REPAIR_SIZE);
            if (!targetRepairSize.isEmpty())
            {
                targetRepairSizeInBytes = UnitConverter.toBytes(targetRepairSize);
            }
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown target repair size specified in '" + CONFIG_REPAIR_TARGET_REPAIR_SIZE + "'", e);
        }

        return new RepairProperties(repairIntervalInMs, repairParallelism, repairAlarmWarnInMs,
                repairAlarmErrorInMs, repairLockType, repairUnwindRatio, repairHistoryLookbackInMs,
                targetRepairSizeInBytes);
    }

    private static long parseTimeUnitToMs(Properties properties, String baseProperty,
                                          String defaultDuration, String defaultTimeUnit) throws ConfigurationException
    {
        String durationProperty = baseProperty + ".time";
        String timeUnitProperty = baseProperty + ".time.unit";

        if (properties.containsKey(durationProperty) != properties.containsKey(timeUnitProperty))
        {
            throw new ConfigurationException("Both TimeUnit and duration of needs to be set or none of them(" + durationProperty + " & " + timeUnitProperty + ")");
        }

        TimeUnit intervalTimeUnit;

        try
        {
            intervalTimeUnit = TimeUnit.valueOf(properties.getProperty(timeUnitProperty, defaultTimeUnit).toUpperCase(Locale.US));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Unknown time unit configured for " + timeUnitProperty, e);
        }

        long interval = Long.parseLong(properties.getProperty(durationProperty, defaultDuration));

        return intervalTimeUnit.toMillis(interval);
    }
}
