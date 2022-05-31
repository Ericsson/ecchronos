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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A representation of a schedule.
 *
 * Primarily used to have a type to convert to JSON.
 */
public class Schedule
{
    public UUID id;
    public String keyspace;
    public String table;
    public RepairJobView.ScheduleStatus status;
    public double repairedRatio;
    public long lastRepairedAtInMs;
    public long nextRepairInMs;
    public ScheduleConfig config;
    public List<VirtualNodeState> virtualNodeStates;

    public Schedule()
    {
    }

    @VisibleForTesting
    public Schedule(UUID id, String keyspace, String table, RepairJobView.ScheduleStatus status, double repairedRatio, long lastRepairedAtInMs, long nextRepairInMs, ScheduleConfig config)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.status = status;
        this.repairedRatio = repairedRatio;
        this.lastRepairedAtInMs = lastRepairedAtInMs;
        this.nextRepairInMs = nextRepairInMs;
        this.config = config;
        this.virtualNodeStates = Collections.emptyList();
    }

    public Schedule(RepairJobView repairJobView)
    {
        this.id = repairJobView.getId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getScheduleStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.lastRepairedAtInMs = repairJobView.getLastCompletedAt();
        this.nextRepairInMs = repairJobView.getNextRepair();
        this.config = new ScheduleConfig(repairJobView);
        this.virtualNodeStates = Collections.emptyList();
    }

    public Schedule(RepairJobView repairJobView, boolean full)
    {
        this(repairJobView);
        if (full)
        {
            long repairedAfter = System.currentTimeMillis() - repairJobView.getRepairConfiguration().getRepairIntervalInMs();
            VnodeRepairStates vnodeRepairStates = repairJobView.getRepairStateSnapshot().getVnodeRepairStates();

            this.virtualNodeStates = vnodeRepairStates.getVnodeRepairStates().stream()
                    .map(vrs -> VirtualNodeState.convert(vrs, repairedAfter))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Schedule that = (Schedule) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs &&
                Double.compare(that.repairedRatio, repairedRatio) == 0 &&
                nextRepairInMs == that.nextRepairInMs &&
                keyspace.equals(that.keyspace) &&
                table.equals(that.table) &&
                status == that.status &&
                id.equals(that.id) &&
                config.equals(that.config) &&
                virtualNodeStates.equals(that.virtualNodeStates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, keyspace, table, lastRepairedAtInMs, repairedRatio, status, nextRepairInMs, config, virtualNodeStates);
    }
}
