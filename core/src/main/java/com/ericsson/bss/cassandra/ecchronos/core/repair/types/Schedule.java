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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.annotations.VisibleForTesting;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
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
@SuppressWarnings("VisibilityModifier")
public class Schedule
{
    @NotBlank
    public UUID nodeID;
    @NotBlank
    public UUID jobID;
    @NotBlank
    public String keyspace;
    @NotBlank
    public String table;
    @NotBlank
    public ScheduledRepairJobView.Status status;
    @NotBlank
    @Min(0)
    @Max(1)
    public double repairedRatio;
    @NotBlank
    public long lastRepairedAtInMs;
    @NotBlank
    public long nextRepairInMs;
    @NotBlank
    public ScheduleConfig config;
    @NotBlank
    public RepairType repairType;
    public List<VirtualNodeState> virtualNodeStates;

    public Schedule()
    {
    }

    @VisibleForTesting
    public Schedule(
            final UUID theNodeID,
            final UUID theJobId,
            final String theKeyspace,
            final String theTable,
            final ScheduledRepairJobView.Status theStatus,
            final double theRepairedRatio,
            final long theLastRepairedAtInMs,
            final long theNextRepairInMs,
            final ScheduleConfig theConfig,
            final RepairType theRepairType
    )
    {
        this.nodeID = theNodeID;
        this.jobID = theJobId;
        this.keyspace = theKeyspace;
        this.table = theTable;
        this.status = theStatus;
        this.repairedRatio = theRepairedRatio;
        this.lastRepairedAtInMs = theLastRepairedAtInMs;
        this.nextRepairInMs = theNextRepairInMs;
        this.config = theConfig;
        this.virtualNodeStates = Collections.emptyList();
        this.repairType = theRepairType;
    }

    public Schedule(final ScheduledRepairJobView repairJobView)
    {
        this.nodeID = repairJobView.getNodeId();
        this.jobID = repairJobView.getJobId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.lastRepairedAtInMs = repairJobView.getCompletionTime();
        this.nextRepairInMs = repairJobView.getNextRepair();
        this.config = new ScheduleConfig(repairJobView);
        this.virtualNodeStates = Collections.emptyList();
        this.repairType = repairJobView.getRepairType();
    }

    public Schedule(final ScheduledRepairJobView repairJobView, final boolean full)
    {
        this(repairJobView);
        if (full)
        {
            long repairedAfter
                    = System.currentTimeMillis() - repairJobView.getRepairConfiguration().getRepairIntervalInMs();
            VnodeRepairStates vnodeRepairStates = repairJobView.getRepairStateSnapshot().getVnodeRepairStates();

            this.virtualNodeStates = vnodeRepairStates.getVnodeRepairStates().stream()
                    .map(vrs -> VirtualNodeState.convert(vrs, repairedAfter))
                    .collect(Collectors.toList());
        }
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
        Schedule that = (Schedule) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs
                && Double.compare(that.repairedRatio, repairedRatio) == 0
                && nextRepairInMs == that.nextRepairInMs
                && keyspace.equals(that.keyspace)
                && table.equals(that.table)
                && status == that.status
                && jobID.equals(that.jobID)
                && nodeID.equals(that.nodeID)
                && config.equals(that.config)
                && virtualNodeStates.equals(that.virtualNodeStates)
                && repairType.equals(that.repairType);
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(nodeID, jobID, keyspace, table, lastRepairedAtInMs, repairedRatio,
                status, nextRepairInMs, config, virtualNodeStates, repairType);
    }
}

