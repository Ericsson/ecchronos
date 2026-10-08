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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Owns the mapping from a table to its single consolidated multi-node {@link UnifiedTableRepairJob}s and
 * encapsulates how per-node configuration events are consolidated into jobs on the {@code UNIFIED_VNODE} path.
 * <p>
 * Every enabled (vnode) configuration for a table is consolidated into a single multi-node job spanning all
 * reporting nodes. Because schema events arrive per node, the set of managed nodes for a table is accumulated
 * as each node reports it, and the single multi-node job is rebuilt whenever that node set (or the
 * configuration set) changes.
 * <p>
 * This type is not thread-safe; callers must serialise access (the owning {@link UnifiedRepairScheduler} does
 * so under a write lock).
 */
final class UnifiedTableScheduleRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedTableScheduleRegistry.class);

    private final Map<TableReference, TableSchedule> myTableJobs = new HashMap<>();
    private final UnifiedRepairJobFactory myJobFactory;
    private final ScheduleManager myScheduleManager;

    UnifiedTableScheduleRegistry(final UnifiedRepairJobFactory jobFactory, final ScheduleManager scheduleManager)
    {
        myJobFactory = jobFactory;
        myScheduleManager = scheduleManager;
    }

    /** Per-table scheduling holder. */
    private static final class TableSchedule
    {
        private final Map<UUID, Node> nodes = new LinkedHashMap<>();
        private final Map<RepairConfiguration, UnifiedTableRepairJob> vnodeJobs = new HashMap<>();

        private boolean isEmpty()
        {
            return vnodeJobs.isEmpty() && nodes.isEmpty();
        }
    }

    /**
     * Apply the (per-node) configuration event for a table. The reporting node is added to the table's node
     * set and the single multi-node job per configuration is (re)built across all reporting nodes.
     *
     * @param node the node reporting the configuration.
     * @param tableReference the table.
     * @param repairConfigurations the enabled (vnode) configurations for the table.
     */
    void applyConfiguration(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        TableSchedule schedule = myTableJobs.computeIfAbsent(tableReference, k -> new TableSchedule());
        boolean newNode = !schedule.nodes.containsKey(node.getHostId());
        schedule.nodes.put(node.getHostId(), node);

        Set<RepairConfiguration> vnodeConfigs = new HashSet<>(repairConfigurations);

        // Only rebuild the multi-node jobs when the node set grew or the configuration set changed; a
        // repeated event from an already-known node with the same configs is a no-op (avoids re-reading
        // RepairState on every schema event).
        if (newNode || !schedule.vnodeJobs.keySet().equals(vnodeConfigs))
        {
            rebuildVnodeJobs(schedule, tableReference, vnodeConfigs);
        }

        removeIfEmpty(tableReference, schedule);
    }

    /**
     * Remove one node's configuration for a table.
     *
     * @param node the node.
     * @param tableReference the table.
     */
    void removeConfiguration(final Node node, final TableReference tableReference)
    {
        TableSchedule schedule = myTableJobs.get(tableReference);
        if (schedule == null)
        {
            LOG.warn("No unified jobs found for table {} when removing config on node {}",
                    tableReference, node.getHostId());
            return;
        }
        removeNode(schedule, tableReference, node.getHostId());
    }

    /**
     * Remove a node from every table it participates in.
     *
     * @param nodeId the node id.
     */
    void removeNode(final UUID nodeId)
    {
        for (Map.Entry<TableReference, TableSchedule> entry : new ArrayList<>(myTableJobs.entrySet()))
        {
            TableSchedule schedule = entry.getValue();
            if (schedule.nodes.containsKey(nodeId))
            {
                removeNode(schedule, entry.getKey(), nodeId);
            }
        }
    }

    /**
     * All consolidated jobs across all tables.
     *
     * @return the list of jobs.
     */
    List<UnifiedTableRepairJob> allJobs()
    {
        List<UnifiedTableRepairJob> jobs = new ArrayList<>();
        for (TableSchedule schedule : myTableJobs.values())
        {
            jobs.addAll(schedule.vnodeJobs.values());
        }
        return jobs;
    }

    /**
     * Deschedule everything and clear the registry.
     */
    void clear()
    {
        for (TableSchedule schedule : myTableJobs.values())
        {
            schedule.vnodeJobs.values().forEach(this::descheduleJob);
        }
        myTableJobs.clear();
    }

    private void rebuildVnodeJobs(
            final TableSchedule schedule,
            final TableReference tableReference,
            final Set<RepairConfiguration> vnodeConfigs)
    {
        schedule.vnodeJobs.keySet().removeIf(config ->
        {
            if (!vnodeConfigs.contains(config))
            {
                descheduleJob(schedule.vnodeJobs.get(config));
                return true;
            }
            return false;
        });

        if (schedule.nodes.isEmpty())
        {
            return;
        }

        Collection<Node> nodes = schedule.nodes.values();
        for (RepairConfiguration config : vnodeConfigs)
        {
            UnifiedTableRepairJob previous = schedule.vnodeJobs.remove(config);
            if (previous != null)
            {
                descheduleJob(previous);
            }
            UnifiedTableRepairJob job = myJobFactory.createMultiNode(nodes, tableReference, config);
            schedule.vnodeJobs.put(config, job);
            myScheduleManager.schedule(anyNode(nodes), job);
            LOG.debug("Scheduled unified vnode repair job for table {} across {} nodes",
                    tableReference, nodes.size());
        }
    }

    private void removeNode(
            final TableSchedule schedule,
            final TableReference tableReference,
            final UUID nodeId)
    {
        schedule.nodes.remove(nodeId);

        if (schedule.nodes.isEmpty())
        {
            schedule.vnodeJobs.values().forEach(this::descheduleJob);
            schedule.vnodeJobs.clear();
        }
        else
        {
            rebuildAllVnodeJobs(schedule, tableReference);
        }

        removeIfEmpty(tableReference, schedule);
    }

    private void rebuildAllVnodeJobs(final TableSchedule schedule, final TableReference tableReference)
    {
        Collection<Node> nodes = schedule.nodes.values();
        Map<RepairConfiguration, UnifiedTableRepairJob> rebuilt = new HashMap<>();
        for (Map.Entry<RepairConfiguration, UnifiedTableRepairJob> entry : schedule.vnodeJobs.entrySet())
        {
            descheduleJob(entry.getValue());
            UnifiedTableRepairJob job = myJobFactory.createMultiNode(nodes, tableReference, entry.getKey());
            rebuilt.put(entry.getKey(), job);
            myScheduleManager.schedule(anyNode(nodes), job);
        }
        schedule.vnodeJobs.clear();
        schedule.vnodeJobs.putAll(rebuilt);
    }

    private void removeIfEmpty(final TableReference tableReference, final TableSchedule schedule)
    {
        if (schedule.isEmpty())
        {
            myTableJobs.remove(tableReference);
        }
    }

    private void descheduleJob(final ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job.getNodeId(), job);
        }
    }

    private static UUID anyNode(final Collection<Node> nodes)
    {
        return nodes.iterator().next().getHostId();
    }
}
