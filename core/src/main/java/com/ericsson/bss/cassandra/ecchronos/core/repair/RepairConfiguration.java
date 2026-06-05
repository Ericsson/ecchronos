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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Configuration options for table repairs.
 */
public final class RepairConfiguration
{
    /** No unwind ratio. */
    public static final double NO_UNWIND = 0.0d;
    /** Full repair size constant. */
    public static final long FULL_REPAIR_SIZE = Long.MAX_VALUE;

    private static final long DEFAULT_REPAIR_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(7);
    private static final long DEFAULT_REPAIR_WARNING_TIME_IN_MS = TimeUnit.DAYS.toMillis(8);
    private static final long DEFAULT_REPAIR_ERROR_TIME_IN_MS = TimeUnit.DAYS.toMillis(10);
    private static final RepairParallelism DEFAULT_REPAIR_PARALLELISM = RepairParallelism.PARALLEL;
    private static final RepairOptions.RepairType DEFAULT_REPAIR_TYPE = RepairOptions.RepairType.VNODE;
    private static final double DEFAULT_UNWIND_RATIO = NO_UNWIND;
    private static final long DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES = FULL_REPAIR_SIZE;

    private static final long DEFAULT_BACKOFF_IN_MS = TimeUnit.MINUTES.toMillis(30);
    private static final boolean DEFAULT_IGNORE_TWCS_TABLES = false;
    private static final long DEFAULT_INITIAL_DELAY_IN_MS = TimeUnit.DAYS.toMillis(1);

    /**
     * Creates a new instance.
     */
    public static final RepairConfiguration DEFAULT = newBuilder().build();
    /**
     * Creates a new instance.
     */
    public static final RepairConfiguration DISABLED = newBuilder()
            .withRepairInterval(0, TimeUnit.MILLISECONDS)
            .build();

    private final RepairOptions.RepairParallelism myRepairParallelism;
    private final long myRepairIntervalInMs;
    private final long myRepairWarningTimeInMs;
    private final long myRepairErrorTimeInMs;
    private final double myRepairUnwindRatio;
    private final long myTargetRepairSizeInBytes;
    private final boolean myIgnoreTWCSTables;
    private final long myBackoffInMs;
    private final TimeUnit myPriorityGranularityUnit;

    private final RepairOptions.RepairType myRepairType;

    private final long myInitialDelayInMs;

    private RepairConfiguration(final Builder builder)
    {
        myRepairParallelism = builder.myRepairParallelism;
        myRepairIntervalInMs = builder.myRepairIntervalInMs;
        myRepairWarningTimeInMs = builder.myRepairWarningTimeInMs;
        myRepairErrorTimeInMs = builder.myRepairErrorTimeInMs;
        myRepairUnwindRatio = builder.myRepairUnwindRatio;
        myTargetRepairSizeInBytes = builder.myTargetRepairSizeInBytes;
        myIgnoreTWCSTables = builder.myIgnoreTWCSTables;
        myBackoffInMs = builder.myBackoffInMs;
        myRepairType = builder.myRepairType;
        myPriorityGranularityUnit = builder.myPriorityGranularityUnit;
        myInitialDelayInMs = builder.myInitialDelayInMs;
    }

    /**
     * Returns the priority granularity unit.
     * @return the priority granularity unit
     */
    public TimeUnit getPriorityGranularityUnit()
    {
        return myPriorityGranularityUnit;
    }

    /**
     * Returns the repair parallelism.
     * @return the repair parallelism
     */
    public RepairOptions.RepairParallelism getRepairParallelism()
    {
        return myRepairParallelism;
    }

    /**
     * Returns the repair interval in ms.
     * @return the repair interval in ms
     */
    public long getRepairIntervalInMs()
    {
        return myRepairIntervalInMs;
    }

    /**
     * Returns the initial delay in ms.
     * @return the initial delay in ms
     */
    public long getInitialDelayInMs()
    {
        return myInitialDelayInMs;
    }

    /**
     * Returns the repair warning time in ms.
     * @return the repair warning time in ms
     */
    public long getRepairWarningTimeInMs()
    {
        return myRepairWarningTimeInMs;
    }

    /**
     * Returns the repair error time in ms.
     * @return the repair error time in ms
     */
    public long getRepairErrorTimeInMs()
    {
        return myRepairErrorTimeInMs;
    }

    /**
     * Returns the repair unwind ratio.
     * @return the repair unwind ratio
     */
    public double getRepairUnwindRatio()
    {
        return myRepairUnwindRatio;
    }

    /**
     * Returns the target repair size in bytes.
     * @return the target repair size in bytes
     */
    public long getTargetRepairSizeInBytes()
    {
        return myTargetRepairSizeInBytes;
    }

    /**
     * Returns the backoff in ms.
     * @return the backoff in ms
     */
    public long getBackoffInMs()
    {
        return myBackoffInMs;
    }

    /**
     * Returns the ignore TWCS tables.
     * @return the ignore TWCS tables
     */
    public boolean getIgnoreTWCSTables()
    {
        return myIgnoreTWCSTables;
    }

    /**
     * Returns the repair type.
     * @return the repair type
     */
    public RepairOptions.RepairType getRepairType()
    {
        return myRepairType;
    }

    /**
     * Creates a new instance.
     * @param from the start of the range
     * @return the built instance
     */
    public static Builder newBuilder(final RepairConfiguration from)
    {
        return new Builder(from);
    }

    /**
     * Creates a new instance.
     * @return the built instance
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return String.format("""
                        RepairConfiguration(interval=%dms,\
                        initialDelay=%dms,\
                        warning=%dms,\
                        error=%dms,\
                        parallelism=%s,\
                        unwindRatio=%.2f,\
                        ignoreTWCS=%b,\
                        backoff=%dms,\
                        repairType=%s,\
                        priorityGranularityUnit=%s))\
                        """,
                        myRepairIntervalInMs,
                        myInitialDelayInMs,
                        myRepairWarningTimeInMs,
                        myRepairErrorTimeInMs,
                        myRepairParallelism,
                        myRepairUnwindRatio,
                        myIgnoreTWCSTables,
                        myBackoffInMs,
                        myRepairType,
                        myPriorityGranularityUnit);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        RepairConfiguration that = (RepairConfiguration) o;
        return myRepairIntervalInMs == that.myRepairIntervalInMs
                && myInitialDelayInMs == that.myInitialDelayInMs
                && myRepairWarningTimeInMs == that.myRepairWarningTimeInMs
                && myRepairErrorTimeInMs == that.myRepairErrorTimeInMs
                && Double.compare(that.myRepairUnwindRatio, myRepairUnwindRatio) == 0
                && myTargetRepairSizeInBytes == that.myTargetRepairSizeInBytes
                && myRepairParallelism == that.myRepairParallelism
                && myIgnoreTWCSTables == that.myIgnoreTWCSTables
                && myBackoffInMs == that.myBackoffInMs
                && myRepairType == that.myRepairType
                && myPriorityGranularityUnit == that.myPriorityGranularityUnit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myRepairParallelism, myRepairIntervalInMs, myInitialDelayInMs, myRepairWarningTimeInMs,
                myRepairErrorTimeInMs, myRepairUnwindRatio, myTargetRepairSizeInBytes, myIgnoreTWCSTables,
                myBackoffInMs, myRepairType, myPriorityGranularityUnit);
    }

    /** Builder for constructing instances of the enclosing class. */
    public static class Builder
    {
        private RepairOptions.RepairParallelism myRepairParallelism = DEFAULT_REPAIR_PARALLELISM;
        private RepairOptions.RepairType myRepairType = DEFAULT_REPAIR_TYPE;
        private long myRepairIntervalInMs = DEFAULT_REPAIR_INTERVAL_IN_MS;
        private long myRepairWarningTimeInMs = DEFAULT_REPAIR_WARNING_TIME_IN_MS;
        private long myRepairErrorTimeInMs = DEFAULT_REPAIR_ERROR_TIME_IN_MS;
        private double myRepairUnwindRatio = DEFAULT_UNWIND_RATIO;
        private long myTargetRepairSizeInBytes = DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES;
        private long myBackoffInMs = DEFAULT_BACKOFF_IN_MS;
        private boolean myIgnoreTWCSTables = DEFAULT_IGNORE_TWCS_TABLES;
        private TimeUnit myPriorityGranularityUnit = TimeUnit.HOURS;


        private long myInitialDelayInMs = DEFAULT_INITIAL_DELAY_IN_MS;

        /**
         * Constructor.
         */
        public Builder()
        {
            // empty
        }

        /**
         * Constructor using repair configuration.
         *
         * @param from Repair configuration.
         */
        public Builder(final RepairConfiguration from)
        {
            myRepairParallelism = from.getRepairParallelism();
            myRepairType = from.getRepairType();
            myRepairIntervalInMs = from.getRepairIntervalInMs();
            myInitialDelayInMs = from.getInitialDelayInMs();
            myRepairWarningTimeInMs = from.getRepairWarningTimeInMs();
            myRepairErrorTimeInMs = from.getRepairErrorTimeInMs();
            myRepairUnwindRatio = from.getRepairUnwindRatio();
            myBackoffInMs = from.getBackoffInMs();
            myPriorityGranularityUnit = from.getPriorityGranularityUnit();
        }

        /**
         * Set the parallelism type to use for repairs.
         *
         * @param parallelism The parallelism.
         * @return The builder
         */
        public Builder withParallelism(final RepairOptions.RepairParallelism parallelism)
        {
            myRepairParallelism = parallelism;
            return this;
        }

        /**
         * Build with repairType.
         *
         * @param repairType The type of repair.
         * @return Builder
         */
        public Builder withRepairType(final RepairOptions.RepairType repairType)
        {
            myRepairType = repairType;
            return this;
        }

        /**
         * Set the repair interval to use.
         *
         * @param repairInterval The repair interval.
         * @param timeUnit The time unit of the repair interval.
         * @return The builder
         */
        public Builder withRepairInterval(final long repairInterval, final TimeUnit timeUnit)
        {
            myRepairIntervalInMs = timeUnit.toMillis(repairInterval);
            return this;
        }

        /**
         * Set the time used to send a <b>warning</b> alarm that repair has not been running correctly.
         *
         * Normally this warning would be sent <b>before gc_grace_seconds</b> has passed to notify the
         * user that some action might need to be taken to continue.
         *
         * @param repairWarningTime The time to use
         * @param timeUnit The time unit
         * @return The builder
         * @see #withRepairErrorTime(long, TimeUnit)
         */
        public Builder withRepairWarningTime(final long repairWarningTime, final TimeUnit timeUnit)
        {
            myRepairWarningTimeInMs = timeUnit.toMillis(repairWarningTime);
            return this;
        }

        /**
         * Set the initial delay period for a new table job.
         *
         * @param initialDelay The time to use as initial delay
         * @param timeUnit The time unit
         * @return The builder
         */
        public Builder withInitialDelay(final long initialDelay, final TimeUnit timeUnit)
        {
            myInitialDelayInMs = timeUnit.toMillis(initialDelay);
            return this;
        }

        /**
         * Set the time used to send an <b>error</b> alarm that repair has not been running correctly.
         *
         * Normally this error would be sent <b>after gc_grace_seconds</b> has passed to notify the
         * user.
         *
         * @param repairErrorTime The time to use
         * @param timeUnit The time unit
         * @return The builder
         * @see #withRepairWarningTime(long, TimeUnit)
         */
        public Builder withRepairErrorTime(final long repairErrorTime, final TimeUnit timeUnit)
        {
            myRepairErrorTimeInMs = timeUnit.toMillis(repairErrorTime);
            return this;
        }

        /**
         * Set the time used to wait after repair has been run.
         *
         * This is used to decrease the pressure repairs create by spreading out repairs over longer periods of time.
         *
         * @param repairUnwindRatio The ratio to use
         * @return The builder
         */
        public Builder withRepairUnwindRatio(final double repairUnwindRatio)
        {
            myRepairUnwindRatio = repairUnwindRatio;
            return this;
        }

        /**
         * Set the target repair size in bytes.
         *
         * This is used to perform sub range repairs within virtual nodes.
         * The sub ranges will be calculated based on how much data is in the table versus how large the target size
         * per repair session is.
         *
         * @param targetRepairSizeInBytes The target data per repair session
         * @return The builder
         */
        public Builder withTargetRepairSizeInBytes(final long targetRepairSizeInBytes)
        {
            myTargetRepairSizeInBytes = targetRepairSizeInBytes;
            return this;
        }

        /**
         * Build with ignore TWCS tables.
         *
         * @param ignore Ignore flag.
         * @return Builder
         */
        public Builder withIgnoreTWCSTables(final boolean ignore)
        {
            myIgnoreTWCSTables = ignore;
            return this;
        }

        /**
         * Build with backoff.
         *
         * @param backoff The backoff
         * @param timeUnit The timeunit for the backoff
         * @return Builder
         */
        public Builder withBackoff(final long backoff, final TimeUnit timeUnit)
        {
            myBackoffInMs = timeUnit.toMillis(backoff);
            return this;
        }

        /**
         * Build with Priority Granularity Unit for the scheduling job.
         *
         * @param unit The Priority Granularity Unit.
         * @return Builder
         */
        public Builder withPriorityGranularityUnit(final TimeUnit unit)
        {
            myPriorityGranularityUnit = unit;
            return this;
        }

        /**
         * Build repair configuration.
         *
         * @return RepairConfiguration
         */
        public RepairConfiguration build()
        {
            return new RepairConfiguration(this);
        }
    }
}
