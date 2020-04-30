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
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.google.common.annotations.VisibleForTesting;

/**
 * A representation of a scheduled repair job.
 *
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
    public final UUID id;

    public enum Status
    {
        COMPLETED, IN_QUEUE, WARNING, ERROR
    }

    @VisibleForTesting
    public ScheduledRepairJob(UUID id, String keyspace, String table, Status status, double repairedRatio, long lastRepairedAtInMs, long nextRepairInMs)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.status = status;
        this.repairedRatio = repairedRatio;
        this.lastRepairedAtInMs = lastRepairedAtInMs;
        this.nextRepairInMs = nextRepairInMs;
    }

    public ScheduledRepairJob(RepairJobView repairJobView)
    {
        this.id = repairJobView.getId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        long now = System.currentTimeMillis();
        if (repairJobView.getRepairStateSnapshot() != null)
        {
            this.lastRepairedAtInMs = repairJobView.getRepairStateSnapshot().lastRepairedAt();
            this.repairedRatio = calculateRepaired(repairJobView, now);
            this.status = getStatus(repairJobView, now);
        }
        else
        {
            this.lastRepairedAtInMs = 0;
            this.repairedRatio = 0;
            this.status = Status.IN_QUEUE;
        }
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScheduledRepairJob that = (ScheduledRepairJob) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs &&
                Double.compare(that.repairedRatio, repairedRatio) == 0 &&
                nextRepairInMs == that.nextRepairInMs &&
                keyspace.equals(that.keyspace) &&
                table.equals(that.table) &&
                status == that.status &&
                id.equals(that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, keyspace, table, lastRepairedAtInMs, repairedRatio, status, nextRepairInMs);
    }
}
