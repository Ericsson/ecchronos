/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * A table-level aggregate view of a consolidated {@code UNIFIED_VNODE} repair job, which spans multiple nodes
 * under a single job id.
 * <p>
 * It summarises the per-node {@link Schedule}s of one job: the {@link #status} is the <em>worst case</em>
 * across nodes (so an operator immediately sees the neediest node), while {@link #repairedRatio} is the
 * <em>average</em> across nodes. The individual node ids are listed in {@link #nodes} so the caller can drill
 * down per node.
 *
 * Primarily used to have a type to convert to JSON.
 */
@SuppressWarnings("VisibilityModifier")
public class UnifiedSchedule
{
    /** The job ID (shared by every node of this unified job). */
    @NotBlank
    public UUID jobID;
    /** The keyspace. */
    @NotBlank
    public String keyspace;
    /** The table. */
    @NotBlank
    public String table;
    /** The worst-case status across all participating nodes. */
    @NotBlank
    public ScheduledRepairJobView.Status status;
    /** The average repaired ratio across all participating nodes. */
    @NotBlank
    @Min(0)
    @Max(1)
    public double repairedRatio;
    /** The oldest last-repaired time (ms) across nodes (the most-behind node). */
    @NotBlank
    public long lastRepairedAtInMs;
    /** The latest next-repair time (ms) across nodes. */
    @NotBlank
    public long nextRepairInMs;
    /** The number of nodes managed by this unified job. */
    @NotBlank
    public int nodeCount;
    /** The ids of the nodes managed by this unified job. */
    public List<UUID> nodes;
    /** The config. */
    @NotBlank
    public ScheduleConfig config;
    /** The repair type (always {@code UNIFIED_VNODE}). */
    @NotBlank
    public RepairType repairType;

    /** Constructs a new UnifiedSchedule. */
    public UnifiedSchedule()
    {
    }

    /**
     * Aggregates the per-node views of a single unified job into one table-level summary.
     *
     * @param jobViews the per-node views of one unified job (all sharing the same job id); must be non-empty.
     */
    public UnifiedSchedule(final List<ScheduledRepairJobView> jobViews)
    {
        if (jobViews == null || jobViews.isEmpty())
        {
            throw new IllegalArgumentException("UnifiedSchedule requires at least one job view");
        }
        ScheduledRepairJobView first = jobViews.get(0);
        this.jobID = first.getJobId();
        this.keyspace = first.getTableReference().getKeyspace();
        this.table = first.getTableReference().getTable();
        this.config = new ScheduleConfig(first);
        this.repairType = first.getRepairType();
        this.nodeCount = jobViews.size();

        this.nodes = new ArrayList<>(jobViews.size());
        ScheduledRepairJobView.Status worstStatus = ScheduledRepairJobView.Status.COMPLETED;
        double ratioSum = 0.0d;
        long oldestLastRepaired = Long.MAX_VALUE;
        long latestNextRepair = Long.MIN_VALUE;
        for (ScheduledRepairJobView view : jobViews)
        {
            this.nodes.add(view.getNodeId());
            worstStatus = worst(worstStatus, view.getStatus());
            ratioSum += view.getProgress();
            oldestLastRepaired = Math.min(oldestLastRepaired, view.getCompletionTime());
            latestNextRepair = Math.max(latestNextRepair, view.getNextRepair());
        }
        this.nodes.sort(Comparator.comparing(UUID::toString));
        this.status = worstStatus;
        this.repairedRatio = ratioSum / jobViews.size();
        this.lastRepairedAtInMs = oldestLastRepaired;
        this.nextRepairInMs = latestNextRepair;
    }

    private static ScheduledRepairJobView.Status worst(
            final ScheduledRepairJobView.Status a,
            final ScheduledRepairJobView.Status b)
    {
        // Status is declared COMPLETED < ON_TIME < LATE < OVERDUE < BLOCKED; higher ordinal = worse.
        return a.ordinal() >= b.ordinal() ? a : b;
    }

    /**
     * Equality.
     *
     * @param o the object to compare to.
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
        UnifiedSchedule that = (UnifiedSchedule) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs
                && Double.compare(that.repairedRatio, repairedRatio) == 0
                && nextRepairInMs == that.nextRepairInMs
                && nodeCount == that.nodeCount
                && jobID.equals(that.jobID)
                && keyspace.equals(that.keyspace)
                && table.equals(that.table)
                && status == that.status
                && nodes.equals(that.nodes)
                && config.equals(that.config)
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
        return Objects.hash(jobID, keyspace, table, status, repairedRatio, lastRepairedAtInMs,
                nextRepairInMs, nodeCount, nodes, config, repairType);
    }
}
