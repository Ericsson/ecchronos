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

import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The consolidated multi-node counterpart of {@code IncrementalRepairJob}, used only for
 * {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType#UNIFIED_INCREMENTAL}.
 * <p>
 * A single {@code UnifiedIncrementalRepairJob} manages the incremental repair of one table across
 * <em>all</em> of its managed nodes, holding one independent {@link IncrementalNodeState} per node. Each node
 * delegate is a fully-featured {@link IncrementalRepairJob}, so incremental semantics are preserved verbatim:
 * one incremental session per node (SSTable anticompaction), one {@code ecchronos.repair_history} read/write
 * per node, and the per-node cross-cycle stall guard. A single logical job coordinates every node instead of
 * one job per node.
 * <p>
 * Scheduling contract (mirrors {@link UnifiedTableRepairJob}):
 * <ul>
 *     <li>{@link #getRealPriority()} is the <em>maximum</em> urgency across nodes (the most-overdue node), so
 *     the job is scheduled as urgently as its neediest node requires.</li>
 *     <li>{@link #iterator()} emits the incremental task of each node whose delegate is currently runnable,
 *     ordered by descending priority (most overdue first).</li>
 *     <li>{@link #refreshState()} refreshes each node independently (per-node throttle).</li>
 *     <li>{@link #postExecute(TaskExecutionResult, ScheduledTask)} routes the result to the delegate that owns
 *     the executed task's node so that per-node history/stall-guard bookkeeping stays correct.</li>
 *     <li>{@link #getViews()} projects one {@link ScheduledRepairJobView} per node for observability, all
 *     carrying this job's id so per-(job, node) units are consistent with the vnode path.</li>
 * </ul>
 */
public class UnifiedIncrementalRepairJob extends ScheduledRepairJob implements UnifiedJob
{
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedIncrementalRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;

    /** Per-node incremental state, keyed by node host id; insertion order preserved. */
    private final Map<UUID, IncrementalNodeState> myNodeStates;

    UnifiedIncrementalRepairJob(final Builder builder)
    {
        super(builder.configuration, builder.tableReference.getId(), builder.primaryNode().getHostId(),
                builder.tableReference, builder.jmxProxyFactory, builder.repairConfiguration,
                builder.repairPolicies, builder.tableRepairMetrics, builder.repairLockType);
        myNodeStates = builder.buildNodeStates();
    }

    /**
     * The representative node delegate for the whole job: the most-overdue node (smallest
     * {@code lastSuccessfulRun}), so the single-view and the base-class scheduling window both reflect the
     * worst case across nodes (consistent with {@link #getRealPriority()}). Falls back to the first managed
     * node when none report a last successful run.
     *
     * @return the representative node delegate.
     */
    private IncrementalRepairJob mostOverdueDelegate()
    {
        IncrementalRepairJob mostOverdue = null;
        long minLastRun = Long.MAX_VALUE;
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            long lastRun = nodeState.getDelegate().getLastSuccessfulRun();
            if (lastRun < minLastRun)
            {
                minLastRun = lastRun;
                mostOverdue = nodeState.getDelegate();
            }
        }
        if (mostOverdue != null)
        {
            return mostOverdue;
        }
        return myNodeStates.values().iterator().next().getDelegate();
    }

    /**
     * Get the managed node ids for this job.
     *
     * @return an immutable set of node host ids.
     */
    @Override
    public Set<UUID> getNodeIds()
    {
        return Collections.unmodifiableSet(myNodeStates.keySet());
    }

    /**
     * Get a single scheduled repair job view for the whole job (the most-overdue node), satisfying the
     * single-view interface contract. Per-node detail is available via {@link #getViews()}.
     *
     * @return the representative (most-overdue) node's view.
     */
    @Override
    public ScheduledRepairJobView getView()
    {
        return rebuildWithJobId(mostOverdueDelegate().getView());
    }

    /**
     * Get one scheduled repair job view per managed node, so observability can distinguish which node is
     * more behind.
     *
     * @return an immutable list of per-node views, in managed-node (insertion) order.
     */
    @Override
    public List<ScheduledRepairJobView> getViews()
    {
        List<ScheduledRepairJobView> views = new ArrayList<>(myNodeStates.size());
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            views.add(rebuildWithJobId(nodeState.getDelegate().getView()));
        }
        return Collections.unmodifiableList(views);
    }

    /**
     * Get the view for a specific managed node, if present.
     *
     * @param nodeId the node host id.
     * @return the node's view, or {@code null} if the node is not managed by this job.
     */
    @Override
    public ScheduledRepairJobView getViewForNode(final UUID nodeId)
    {
        IncrementalNodeState nodeState = myNodeStates.get(nodeId);
        return nodeState == null ? null : rebuildWithJobId(nodeState.getDelegate().getView());
    }

    /**
     * Rebuild a delegate's view so it carries this consolidated job's id (keeping per-(job, node) identity
     * consistent with the vnode path) while preserving the delegate's per-node status/progress/timing.
     */
    private ScheduledRepairJobView rebuildWithJobId(final ScheduledRepairJobView delegateView)
    {
        return new ScheduledRepairJobView(delegateView.getNodeId(), getJobId(), getTableReference(),
                delegateView.getRepairConfiguration(), delegateView.getStatus(), delegateView.getProgress(),
                delegateView.getNextRepair(), delegateView.getCompletionTime(), delegateView.getRepairType());
    }

    /**
     * Iterator for scheduled tasks built across all managed nodes, ordered most-overdue first.
     *
     * @return Scheduled task iterator
     */
    @Override
    public Iterator<ScheduledTask> iterator()
    {
        List<ScheduledTask> taskList = new ArrayList<>();
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            IncrementalRepairJob delegate = nodeState.getDelegate();
            if (delegate.getRealPriority() == -1)
            {
                continue;
            }
            delegate.iterator().forEachRemaining(taskList::add);
        }
        if (taskList.isEmpty())
        {
            return Collections.emptyIterator();
        }
        // Attempt the most-overdue units first. The job's scheduling priority is the max across nodes;
        // this ordering only governs the sequence of attempts once the job is selected to run.
        taskList.sort(Comparator.comparingInt(ScheduledTask::getPriority).reversed());
        return taskList.iterator();
    }

    /**
     * Last successful run = the most-overdue node's last successful run, so the base-class scheduling window
     * reflects the worst case across nodes (consistent with {@link #getRealPriority()}).
     *
     * @return long
     */
    @Override
    public long getLastSuccessfulRun()
    {
        return mostOverdueDelegate().getLastSuccessfulRun();
    }

    /**
     * Runnable when any managed node delegate is runnable.
     *
     * @return boolean
     */
    @Override
    public boolean runnable()
    {
        return anyNodeRunnable() && super.runnable();
    }

    private boolean anyNodeRunnable()
    {
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            if (nodeState.getDelegate().getRealPriority() != -1)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Route the execution result to the delegate owning the executed task's node so per-node history and
     * stall-guard bookkeeping stays correct, then reset every node's refresh throttle.
     *
     * @param result the result of the task execution.
     * @param task the task that was executed.
     */
    @Override
    public final void postExecute(final TaskExecutionResult result, final ScheduledTask task)
    {
        super.postExecute(result, task);
        UUID taskNodeId = task.getNodeId();
        if (taskNodeId != null)
        {
            IncrementalNodeState owner = myNodeStates.get(taskNodeId);
            if (owner != null)
            {
                owner.getDelegate().postExecute(result, task);
            }
            else
            {
                LOG.warn("{} - postExecute for unknown node {}, skipping per-node bookkeeping", this, taskNodeId);
            }
        }
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            nodeState.resetRefreshThrottle();
        }
    }

    /**
     * Refresh the repair state of every managed node delegate (each independently throttled).
     */
    @Override
    public void refreshState()
    {
        long now = System.currentTimeMillis();
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            if (!nodeState.tryClaimRefresh(now))
            {
                continue;
            }
            try
            {
                nodeState.getDelegate().refreshState();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to refresh incremental state for node {}, {}", nodeState.getHostId(), this, e);
            }
        }
    }

    /**
     * Priority = the most-overdue node's priority (max urgency across nodes).
     *
     * @return priority
     */
    @Override
    public final int getRealPriority()
    {
        int maxPriority = -1;
        for (IncrementalNodeState nodeState : myNodeStates.values())
        {
            int priority = nodeState.getDelegate().getRealPriority();
            if (priority > maxPriority)
            {
                maxPriority = priority;
            }
        }
        return maxPriority;
    }

    @Override
    public final String toString()
    {
        return String.format("Unified incremental repair job of %s across %d nodes", getTableReference(),
                myNodeStates.size());
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }
        UnifiedIncrementalRepairJob that = (UnifiedIncrementalRepairJob) o;
        return Objects.equals(myNodeStates, that.myNodeStates);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myNodeStates);
    }

    /**
     * Builder for constructing {@link UnifiedIncrementalRepairJob} instances.
     */
    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS)
                .build();
        private final Map<UUID, IncrementalNodeState> nodeStates = new LinkedHashMap<>();
        private TableReference tableReference;
        private DistributedJmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private final List<TableRepairPolicy> repairPolicies = new ArrayList<>();
        private RepairLockType repairLockType;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Add a per-node incremental state (one per managed node).
         *
         * @param nodeState the per-node incremental state. Must not be {@code null}.
         * @return this builder.
         */
        public Builder withNodeState(final IncrementalNodeState nodeState)
        {
            Preconditions.checkNotNull(nodeState, "Incremental node state must be set");
            this.nodeStates.put(nodeState.getHostId(), nodeState);
            return this;
        }

        /**
         * Build with configuration.
         *
         * @param theConfiguration Configuration.
         * @return Builder
         */
        public Builder withConfiguration(final Configuration theConfiguration)
        {
            this.configuration = theConfiguration;
            return this;
        }

        /**
         * Build with repair lock type.
         *
         * @param theRepairLockType Repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType theRepairLockType)
        {
            this.repairLockType = theRepairLockType;
            return this;
        }

        /**
         * Build with table reference.
         *
         * @param theTableReference Table reference.
         * @return Builder
         */
        public Builder withTableReference(final TableReference theTableReference)
        {
            this.tableReference = theTableReference;
            return this;
        }

        /**
         * Build with JMX proxy factory.
         *
         * @param aJmxProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory aJmxProxyFactory)
        {
            this.jmxProxyFactory = aJmxProxyFactory;
            return this;
        }

        /**
         * Build with table repair metrics.
         *
         * @param theTableRepairMetrics Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics theTableRepairMetrics)
        {
            this.tableRepairMetrics = theTableRepairMetrics;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param theRepairConfiguration The repair configuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration theRepairConfiguration)
        {
            this.repairConfiguration = theRepairConfiguration;
            return this;
        }

        /**
         * Build with repair policies.
         *
         * @param tableRepairPolicies The table repair policies.
         * @return Builder
         */
        public Builder withRepairPolices(final Collection<TableRepairPolicy> tableRepairPolicies)
        {
            this.repairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        final com.datastax.oss.driver.api.core.metadata.Node primaryNode()
        {
            Preconditions.checkArgument(!nodeStates.isEmpty(),
                    "At least one incremental node state must be set");
            return nodeStates.values().iterator().next().getNode();
        }

        final Map<UUID, IncrementalNodeState> buildNodeStates()
        {
            return Collections.unmodifiableMap(new LinkedHashMap<>(nodeStates));
        }

        /**
         * Build the unified incremental repair job.
         *
         * @return UnifiedIncrementalRepairJob
         */
        public UnifiedIncrementalRepairJob build()
        {
            Preconditions.checkNotNull(tableReference, "Table reference must be set");
            Preconditions.checkArgument(!nodeStates.isEmpty(),
                    "At least one incremental node state must be set");
            return new UnifiedIncrementalRepairJob(this);
        }
    }
}
