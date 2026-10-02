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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;

import java.util.Collection;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Builds per-node {@link ScheduledRepairJobView}s for a {@link UnifiedTableRepairJob}, keeping the
 * status/progress/next-run derivation out of the job class itself.
 */
final class UnifiedNodeViews
{
    private final UUID jobId;
    private final TableReference tableReference;
    private final RepairConfiguration repairConfiguration;
    private final RepairType repairType;
    /** Delegates status classification to the job's base-class {@code classifyStatus}. */
    private final BiFunction<Long, Long, ScheduledRepairJobView.Status> statusClassifier;

    UnifiedNodeViews(
            final UUID theJobId,
            final TableReference theTableReference,
            final RepairConfiguration theRepairConfiguration,
            final BiFunction<Long, Long, ScheduledRepairJobView.Status> theStatusClassifier)
    {
        this.jobId = theJobId;
        this.tableReference = theTableReference;
        this.repairConfiguration = theRepairConfiguration;
        this.repairType = theRepairConfiguration.getRepairType();
        this.statusClassifier = theStatusClassifier;
    }

    ScheduledRepairJobView build(final NodeRepairState nodeRepairState)
    {
        long now = System.currentTimeMillis();
        RepairStateSnapshot snapshot = nodeRepairState.getSnapshot();
        return new ScheduledRepairJobView(nodeRepairState.getHostId(), jobId, tableReference, repairConfiguration,
                snapshot, statusClassifier.apply(now, snapshot.lastCompletedAt()), progress(now, snapshot),
                nextRunInMs(snapshot), repairType);
    }

    private long nextRunInMs(final RepairStateSnapshot snapshot)
    {
        return (snapshot.lastCompletedAt() + repairConfiguration.getRepairIntervalInMs())
                - snapshot.getEstimatedRepairTime();
    }

    private double progress(final long timestamp, final RepairStateSnapshot snapshot)
    {
        long interval = repairConfiguration.getRepairIntervalInMs();
        Collection<VnodeRepairState> states = snapshot.getVnodeRepairStates().getVnodeRepairStates();
        long nRepaired = states.stream()
                .filter(state -> timestamp - state.lastRepairedAt() <= interval)
                .count();
        return states.isEmpty() ? 0 : (double) nRepaired / states.size();
    }
}
