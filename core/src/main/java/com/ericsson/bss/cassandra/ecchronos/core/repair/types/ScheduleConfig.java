/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.Objects;

/**
 * A representation of a table repair configuration.
 *
 * Primarily used to have a type to convert to JSON.
 */
@SuppressWarnings("VisibilityModifier")
public class ScheduleConfig
{
    @NotBlank
    @Min(0)
    public long intervalInMs;
    @NotBlank
    @Min(0)
    public double unwindRatio;
    @NotBlank
    @Min(0)
    public long warningTimeInMs;
    @NotBlank
    @Min(0)
    public long errorTimeInMs;
    @NotBlank
    public RepairParallelism parallelism;

    public ScheduleConfig()
    {
    }

    public ScheduleConfig(final ScheduledRepairJobView repairJobView)
    {
        RepairConfiguration config = repairJobView.getRepairConfiguration();

        this.intervalInMs = config.getRepairIntervalInMs();
        this.unwindRatio = config.getRepairUnwindRatio();
        this.warningTimeInMs = config.getRepairWarningTimeInMs();
        this.errorTimeInMs = config.getRepairErrorTimeInMs();
        this.parallelism = config.getRepairParallelism();
    }

    /**
     * Equality.
     *
     * @param o The object to compare to.
     * @return boolean
     */
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
        ScheduleConfig that = (ScheduleConfig) o;
        return  intervalInMs == that.intervalInMs
                && Double.compare(that.unwindRatio, unwindRatio) == 0
                && warningTimeInMs == that.warningTimeInMs
                && errorTimeInMs == that.errorTimeInMs
                && parallelism == that.parallelism;
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects
                .hash(intervalInMs, unwindRatio, warningTimeInMs, errorTimeInMs, parallelism);
    }
}
