/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import java.util.Collection;
import java.util.function.Predicate;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;

/**
 * A representation of a scheduled repair job.
 * <p>
 * Primarily used to to have a type to convert to JSON.
 */
public class ScheduledRepairJob
{
    public final String keyspace;
    public final String table;
    public final long lastRepairedAtInMs;
    public final double repairedRatio;
    public final Status status;
    public final long nextRepairInMs;

    public enum Status
    {
        COMPLETED, IN_QUEUE, WARNING, ERROR
    }

    public ScheduledRepairJob(RepairJobView repairJobView)
    {
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.lastRepairedAtInMs = repairJobView.getRepairStateSnapshot().lastRepairedAt();
        long now = System.currentTimeMillis();
        this.repairedRatio = calculateRepaired(repairJobView, now);
        this.status = getStatus(repairJobView, now);
        this.nextRepairInMs = lastRepairedAtInMs + repairJobView.getRepairConfiguration().getRepairIntervalInMs();
    }

    private double calculateRepaired(RepairJobView job, long timestamp)
    {
        long interval = job.getRepairConfiguration().getRepairIntervalInMs();
        Collection<VnodeRepairState> states = job.getRepairStateSnapshot().getVnodeRepairStates().getVnodeRepairStates();

        long nRepaired = states.stream()
                .filter(isRepaired(timestamp, interval))
                .count();

        return states.isEmpty()
                ? 0
                : (double) nRepaired / states.size();
    }

    private Predicate<VnodeRepairState> isRepaired(long timestamp, long interval)
    {
        return state -> timestamp - state.lastRepairedAt() <= interval;
    }

    private Status getStatus(RepairJobView job, long timestamp)
    {
        long repairedAt = job.getRepairStateSnapshot().lastRepairedAt();
        long msSinceLastRepair = timestamp - repairedAt;
        RepairConfiguration config = job.getRepairConfiguration();

        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return Status.ERROR;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return Status.WARNING;
        }
        if (msSinceLastRepair >= config.getRepairIntervalInMs())
        {
            return Status.IN_QUEUE;
        }
        return Status.COMPLETED;
    }
}
