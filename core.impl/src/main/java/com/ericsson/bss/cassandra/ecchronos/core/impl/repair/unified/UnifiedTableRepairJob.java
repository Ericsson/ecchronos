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
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
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
 * The consolidated multi-node counterpart of {@code TableRepairJob}, used only for
 * {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType#UNIFIED_VNODE}.
 * <p>
 * A single {@code UnifiedTableRepairJob} manages the repair of one table across <em>all</em> of its managed
 * nodes, holding one independent {@link NodeRepairState} per node. Repair execution is identical to a vnode
 * repair (per token sub-range), but a single logical job coordinates every node instead of one job per node.
 * <p>
 * Scheduling contract:
 * <ul>
 *     <li>{@link #getRealPriority()} is the <em>maximum</em> urgency across nodes (the most-overdue node), so
 *     the job is scheduled as urgently as its neediest node requires — not the sum, which would starve
 *     single-node-critical tables.</li>
 *     <li>{@link #iterator()} emits one {@link RepairGroup} per (node, replica group) for nodes whose state
 *     currently {@code canRepair()}, ordered by descending priority (most overdue first).</li>
 *     <li>{@link #refreshState()} refreshes each node independently (per-node throttle).</li>
 *     <li>{@link #getViews()} projects one {@link ScheduledRepairJobView} per node for observability.</li>
 * </ul>
 */
public class UnifiedTableRepairJob extends ScheduledRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedTableRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;

    /** Per-node repair state, keyed by node host id; insertion order preserved. */
    private final Map<UUID, NodeRepairState> myNodeRepairStates;
    private final TableStorageStates myTableStorageStates;
    private final RepairHistoryService myRepairHistory;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final UnifiedNodeViews myNodeViews;

    UnifiedTableRepairJob(final Builder builder)
    {
        super(builder.configuration, builder.tableReference.getId(), builder.primaryNode().getHostId(),
                builder.tableReference, builder.jmxProxyFactory, builder.repairConfiguration,
                builder.repairPolicies, builder.tableRepairMetrics, builder.repairLockType);
        myNodeRepairStates = builder.buildNodeRepairStates();
        myTableStorageStates = builder.tableStorageStates;
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory, "Repair history must be set");
        myTimeBasedRunPolicy = Preconditions.checkNotNull(builder.myTimeBasedRunPolicy,
                "TimeBasedRunPolicy must be set");
        myNodeViews = new UnifiedNodeViews(builder.tableReference.getId(), builder.tableReference,
                builder.repairConfiguration, this::classifyStatus);
    }

    /**
     * The representative node state for the whole job: the most-overdue node (smallest
     * {@code lastCompletedAt}) among nodes that currently {@code canRepair()}, so the single-view and the
     * base-class scheduling window both reflect the worst case across nodes (consistent with
     * {@link #getRealPriority()}). Falls back to the first managed node when no node can currently repair.
     *
     * @return the representative node repair state.
     */
    private NodeRepairState mostOverdueNodeState()
    {
        NodeRepairState mostOverdue = null;
        long minCompletedAt = Long.MAX_VALUE;
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            RepairStateSnapshot snapshot = nodeRepairState.getSnapshot();
            if (!snapshot.canRepair())
            {
                continue;
            }
            if (snapshot.lastCompletedAt() < minCompletedAt)
            {
                minCompletedAt = snapshot.lastCompletedAt();
                mostOverdue = nodeRepairState;
            }
        }
        if (mostOverdue != null)
        {
            return mostOverdue;
        }
        return myNodeRepairStates.values().iterator().next();
    }

    /**
     * Get the managed node ids for this job.
     *
     * @return an immutable set of node host ids.
     */
    public Set<UUID> getNodeIds()
    {
        return Collections.unmodifiableSet(myNodeRepairStates.keySet());
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
        return buildViewForNode(mostOverdueNodeState());
    }

    /**
     * Get one scheduled repair job view per managed node, so observability can distinguish which node is
     * more behind.
     *
     * @return an immutable list of per-node views, in managed-node (insertion) order.
     */
    public List<ScheduledRepairJobView> getViews()
    {
        List<ScheduledRepairJobView> views = new ArrayList<>(myNodeRepairStates.size());
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            views.add(buildViewForNode(nodeRepairState));
        }
        return Collections.unmodifiableList(views);
    }

    /**
     * Get the view for a specific managed node, if present.
     *
     * @param nodeId the node host id.
     * @return the node's view, or {@code null} if the node is not managed by this job.
     */
    public ScheduledRepairJobView getViewForNode(final UUID nodeId)
    {
        NodeRepairState nodeRepairState = myNodeRepairStates.get(nodeId);
        return nodeRepairState == null ? null : buildViewForNode(nodeRepairState);
    }

    private ScheduledRepairJobView buildViewForNode(final NodeRepairState nodeRepairState)
    {
        return myNodeViews.build(nodeRepairState);
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
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            addTasksForNode(taskList, nodeRepairState);
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

    private void addTasksForNode(final List<ScheduledTask> taskList, final NodeRepairState nodeRepairState)
    {
        RepairStateSnapshot repairStateSnapshot = nodeRepairState.getSnapshot();
        if (!repairStateSnapshot.canRepair())
        {
            return;
        }
        BigInteger tokensPerRepair = getTokensPerRepair(nodeRepairState, repairStateSnapshot.getVnodeRepairStates());
        for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
        {
            RepairGroup.Builder builder = RepairGroup.newBuilder()
                    .withTableReference(getTableReference())
                    .withRepairConfiguration(getRepairConfiguration())
                    .withReplicaRepairGroup(replicaRepairGroup)
                    .withJmxProxyFactory(getJmxProxyFactory())
                    .withTableRepairMetrics(getTableRepairMetrics())
                    .withTokensPerRepair(tokensPerRepair)
                    .withRepairPolicies(getRepairPolicies())
                    .withRepairHistory(myRepairHistory)
                    .withRepairResourceFactory(getRepairLockType().getLockFactory())
                    .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                    .withJobId(getJobId())
                    .withNode(nodeRepairState.getNode())
                    .withTimeBasedRunPolicy(myTimeBasedRunPolicy);
            taskList.add(builder.build(getRealPriority(replicaRepairGroup.lastCompletedAt())));
        }
    }

    /**
     * Last successful run = the most-overdue node's last completed time, so the base-class scheduling window
     * reflects the worst case across nodes (consistent with {@link #getRealPriority()}).
     *
     * @return long
     */
    @Override
    public long getLastSuccessfulRun()
    {
        return mostOverdueNodeState().getSnapshot().lastCompletedAt();
    }

    /**
     * Run offset = the most-overdue node's estimated repair time, matching the node used for
     * {@link #getLastSuccessfulRun()}.
     *
     * @return long
     */
    @Override
    public long getRunOffset()
    {
        return mostOverdueNodeState().getSnapshot().getEstimatedRepairTime();
    }

    /**
     * Runnable when any managed node can repair.
     *
     * @return boolean
     */
    @Override
    public boolean runnable()
    {
        return anyNodeCanRepair() && super.runnable();
    }

    private boolean anyNodeCanRepair()
    {
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            if (nodeRepairState.getSnapshot().canRepair())
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public final void postExecute(final boolean successful, final ScheduledTask task)
    {
        super.postExecute(successful, task);
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            nodeRepairState.resetRefreshThrottle();
        }
    }

    /**
     * Refresh the repair state of every managed node (each independently throttled).
     */
    @Override
    public void refreshState()
    {
        long now = System.currentTimeMillis();
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            if (!nodeRepairState.tryClaimRefresh(now))
            {
                continue;
            }
            try
            {
                nodeRepairState.getRepairState().update();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to check repair history for node {}, {}", nodeRepairState.getHostId(), this, e);
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
        long minRepairedAt = System.currentTimeMillis();
        boolean canRepair = false;
        for (NodeRepairState nodeRepairState : myNodeRepairStates.values())
        {
            RepairStateSnapshot repairStateSnapshot = nodeRepairState.getSnapshot();
            if (!repairStateSnapshot.canRepair())
            {
                continue;
            }
            canRepair = true;
            for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                long replicaGroupCompletedAt = replicaRepairGroup.lastCompletedAt();
                if (replicaGroupCompletedAt < minRepairedAt)
                {
                    minRepairedAt = replicaGroupCompletedAt;
                }
            }
        }
        return canRepair ? getRealPriority(minRepairedAt) : -1;
    }

    @Override
    public final String toString()
    {
        return String.format("Unified repair job of %s across %d nodes", getTableReference(),
                myNodeRepairStates.size());
    }

    private BigInteger getTokensPerRepair(final NodeRepairState nodeRepairState,
            final VnodeRepairStates vnodeRepairStates)
    {
        BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;
        if (getRepairConfiguration().getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            BigInteger tableSizeInBytes = BigInteger.valueOf(
                    myTableStorageStates.getDataSize(nodeRepairState.getHostId(), getTableReference()));
            if (!BigInteger.ZERO.equals(tableSizeInBytes))
            {
                BigInteger fullRangeSize = vnodeRepairStates.getVnodeRepairStates().stream()
                        .map(VnodeRepairState::getTokenRange)
                        .map(LongTokenRange::rangeSize)
                        .reduce(BigInteger.ZERO, BigInteger::add);
                BigInteger targetSizeInBytes = BigInteger.valueOf(
                        getRepairConfiguration().getTargetRepairSizeInBytes());
                if (tableSizeInBytes.compareTo(targetSizeInBytes) > 0)
                {
                    BigInteger targetRepairs = tableSizeInBytes.divide(targetSizeInBytes);
                    tokensPerRepair = fullRangeSize.divide(targetRepairs);
                }
            }
        }
        return tokensPerRepair;
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
        UnifiedTableRepairJob that = (UnifiedTableRepairJob) o;
        return Objects.equals(myNodeRepairStates, that.myNodeRepairStates)
                && Objects.equals(myTableStorageStates, that.myTableStorageStates)
                && Objects.equals(myRepairHistory, that.myRepairHistory)
                && Objects.equals(myTimeBasedRunPolicy, that.myTimeBasedRunPolicy);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myNodeRepairStates, myTableStorageStates, myRepairHistory,
                myTimeBasedRunPolicy);
    }

    /**
     * Builder for constructing {@link UnifiedTableRepairJob} instances.
     */
    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS)
                .build();
        private final Map<UUID, NodeRepairState> nodeRepairStates = new LinkedHashMap<>();
        private TableReference tableReference;
        private DistributedJmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private TableStorageStates tableStorageStates;
        private final List<TableRepairPolicy> repairPolicies = new ArrayList<>();
        private RepairHistoryService repairHistory;
        private RepairLockType repairLockType;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Add a per-node repair state (one per managed node).
         *
         * @param nodeRepairState the per-node repair state. Must not be {@code null}.
         * @return this builder.
         */
        public Builder withNodeRepairState(final NodeRepairState nodeRepairState)
        {
            Preconditions.checkNotNull(nodeRepairState, "Node repair state must be set");
            this.nodeRepairStates.put(nodeRepairState.getHostId(), nodeRepairState);
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
         * Build with table storage states.
         *
         * @param theTableStorageStates Table storage states.
         * @return Builder
         */
        public Builder withTableStorageStates(final TableStorageStates theTableStorageStates)
        {
            this.tableStorageStates = theTableStorageStates;
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

        /**
         * Build with repair history.
         *
         * @param aRepairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistoryService aRepairHistory)
        {
            this.repairHistory = aRepairHistory;
            return this;
        }

        /**
         * Build with TimeBasedRunPolicy.
         *
         * @param timeBasedRunPolicy TimeBasedRunPolicy.
         * @return Builder
         */
        public Builder withTimeBasedRunPolicy(final TimeBasedRunPolicy timeBasedRunPolicy)
        {
            myTimeBasedRunPolicy = timeBasedRunPolicy;
            return this;
        }

        final com.datastax.oss.driver.api.core.metadata.Node primaryNode()
        {
            Preconditions.checkArgument(!nodeRepairStates.isEmpty(),
                    "At least one node repair state must be set");
            return nodeRepairStates.values().iterator().next().getNode();
        }

        final Map<UUID, NodeRepairState> buildNodeRepairStates()
        {
            return Collections.unmodifiableMap(new LinkedHashMap<>(nodeRepairStates));
        }

        /**
         * Build the unified table repair job.
         *
         * @return UnifiedTableRepairJob
         */
        public UnifiedTableRepairJob build()
        {
            Preconditions.checkNotNull(tableReference, "Table reference must be set");
            Preconditions.checkArgument(!nodeRepairStates.isEmpty(),
                    "At least one node repair state must be set");
            return new UnifiedTableRepairJob(this);
        }
    }
}
