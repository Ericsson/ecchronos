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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Configuration options for table repairs.
 */
public class RepairConfiguration
{
    public static final double NO_UNWIND = 0.0d;

    private static final long DEFAULT_REPAIR_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(7);
    private static final long DEFAULT_REPAIR_WARNING_TIME_IN_MS = TimeUnit.DAYS.toMillis(8);
    private static final long DEFAULT_REPAIR_ERROR_TIME_IN_MS = TimeUnit.DAYS.toMillis(10);
    private static final RepairOptions.RepairParallelism DEFAULT_REPAIR_PARALLELISM = RepairOptions.RepairParallelism.PARALLEL;
    private static final double DEFAULT_UNWIND_RATIO = NO_UNWIND;

    public static final RepairConfiguration DEFAULT = newBuilder().build();

    private final RepairOptions.RepairParallelism myRepairParallelism;
    private final long myRepairIntervalInMs;
    private final long myRepairWarningTimeInMs;
    private final long myRepairErrorTimeInMs;
    private final double myRepairUnwindRatio;

    private RepairConfiguration(Builder builder)
    {
        myRepairParallelism = builder.myRepairParallelism;
        myRepairIntervalInMs = builder.myRepairIntervalInMs;
        myRepairWarningTimeInMs = builder.myRepairWarningTimeInMs;
        myRepairErrorTimeInMs = builder.myRepairErrorTimeInMs;
        myRepairUnwindRatio = builder.myRepairUnwindRatio;
    }

    public RepairOptions.RepairParallelism getRepairParallelism()
    {
        return myRepairParallelism;
    }

    public long getRepairIntervalInMs()
    {
        return myRepairIntervalInMs;
    }

    public long getRepairWarningTimeInMs()
    {
        return myRepairWarningTimeInMs;
    }

    public long getRepairErrorTimeInMs()
    {
        return myRepairErrorTimeInMs;
    }

    public double getRepairUnwindRatio()
    {
        return myRepairUnwindRatio;
    }

    public static Builder newBuilder(RepairConfiguration from)
    {
        return new Builder(from);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return String.format("RepairConfiguration(interval=%dms,warning=%dms,error=%dms,parallelism=%s,unwindRatio=%.2f)",
                myRepairIntervalInMs,
                myRepairWarningTimeInMs,
                myRepairErrorTimeInMs,
                myRepairParallelism,
                myRepairUnwindRatio);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairConfiguration that = (RepairConfiguration) o;
        return myRepairIntervalInMs == that.myRepairIntervalInMs &&
                myRepairWarningTimeInMs == that.myRepairWarningTimeInMs &&
                myRepairErrorTimeInMs == that.myRepairErrorTimeInMs &&
                myRepairParallelism == that.myRepairParallelism;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myRepairParallelism, myRepairIntervalInMs, myRepairWarningTimeInMs, myRepairErrorTimeInMs);
    }

    public static class Builder
    {
        private RepairOptions.RepairParallelism myRepairParallelism = DEFAULT_REPAIR_PARALLELISM;
        private long myRepairIntervalInMs = DEFAULT_REPAIR_INTERVAL_IN_MS;
        private long myRepairWarningTimeInMs = DEFAULT_REPAIR_WARNING_TIME_IN_MS;
        private long myRepairErrorTimeInMs = DEFAULT_REPAIR_ERROR_TIME_IN_MS;
        private double myRepairUnwindRatio = DEFAULT_UNWIND_RATIO;

        public Builder()
        {
            // empty
        }

        public Builder(RepairConfiguration from)
        {
            myRepairParallelism = from.getRepairParallelism();
            myRepairIntervalInMs = from.getRepairIntervalInMs();
            myRepairWarningTimeInMs = from.getRepairWarningTimeInMs();
            myRepairErrorTimeInMs = from.getRepairErrorTimeInMs();
        }

        /**
         * Set the parallelism type to use for repairs.
         *
         * @param parallelism The parallelism
         * @return The builder
         */
        public Builder withParallelism(RepairOptions.RepairParallelism parallelism)
        {
            myRepairParallelism = parallelism;
            return this;
        }

        /**
         * Set the repair interval to use.
         *
         * @param repairInterval The repair interval.
         * @param timeUnit The time unit of the repair interval.
         * @return The builder
         */
        public Builder withRepairInterval(long repairInterval, TimeUnit timeUnit)
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
        public Builder withRepairWarningTime(long repairWarningTime, TimeUnit timeUnit)
        {
            myRepairWarningTimeInMs = timeUnit.toMillis(repairWarningTime);
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
        public Builder withRepairErrorTime(long repairErrorTime, TimeUnit timeUnit)
        {
            myRepairErrorTimeInMs = timeUnit.toMillis(repairErrorTime);
            return this;
        }

        public Builder withRepairUnwindRatio(double repairUnwindRatio)
        {
            myRepairUnwindRatio = repairUnwindRatio;
            return this;
        }

        public RepairConfiguration build()
        {
            return new RepairConfiguration(this);
        }
    }
}
