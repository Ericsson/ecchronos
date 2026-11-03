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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Common per-node observability contract shared by the consolidated multi-node repair jobs
 * ({@link UnifiedTableRepairJob} and {@link UnifiedIncrementalRepairJob}).
 * <p>
 * The {@link UnifiedRepairScheduler} projects per-node views for the REST {@code unified-schedules} endpoint
 * and {@code ecctool unified-schedules} through this interface, independent of whether the underlying job is
 * a vnode or an incremental unified job.
 */
public interface UnifiedJob
{
    /**
     * Get the managed node ids for this job.
     *
     * @return an immutable set of node host ids.
     */
    Set<UUID> getNodeIds();

    /**
     * Get one scheduled repair job view per managed node.
     *
     * @return an immutable list of per-node views.
     */
    List<ScheduledRepairJobView> getViews();

    /**
     * Get the view for a specific managed node, if present.
     *
     * @param nodeId the node host id.
     * @return the node's view, or {@code null} if the node is not managed by this job.
     */
    ScheduledRepairJobView getViewForNode(UUID nodeId);
}
