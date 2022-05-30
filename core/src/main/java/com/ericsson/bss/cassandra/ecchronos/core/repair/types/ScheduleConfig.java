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
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;

import java.util.Objects;

/**
 * A representation of a table repair configuration.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class ScheduleConfig
{
    public long intervalInMs;
    public RepairParallelism parallelism;
    public double unwindRatio;
    public long warningTimeInMs;
    public long errorTimeInMs;

    public ScheduleConfig()
    {
    }

    public ScheduleConfig(RepairJobView repairJobView)
    {
        RepairConfiguration config = repairJobView.getRepairConfiguration();

        this.intervalInMs = config.getRepairIntervalInMs();
        this.parallelism = config.getRepairParallelism();
        this.unwindRatio = config.getRepairUnwindRatio();
        this.warningTimeInMs = config.getRepairWarningTimeInMs();
        this.errorTimeInMs = config.getRepairErrorTimeInMs();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScheduleConfig that = (ScheduleConfig) o;
        return  intervalInMs == that.intervalInMs &&
                Double.compare(that.unwindRatio, unwindRatio) == 0 &&
                warningTimeInMs == that.warningTimeInMs &&
                errorTimeInMs == that.errorTimeInMs &&
                parallelism == that.parallelism;
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash(intervalInMs, parallelism, unwindRatio, warningTimeInMs,
                        errorTimeInMs);
    }
}
