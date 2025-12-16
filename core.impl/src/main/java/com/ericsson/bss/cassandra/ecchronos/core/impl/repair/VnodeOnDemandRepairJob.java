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

package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairGroupFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A Job that will schedule and run vnode repair on one table once. It creates VnodeRepairTasks
 * to fully repair the table for the current node. Once all VnodeRepairTask VnodeRepairTasks are completed,
 * the repair is finished and the job will be descheduled.
 */
public final class VnodeOnDemandRepairJob extends OnDemandRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(VnodeOnDemandRepairJob.class);
    private final RepairHistory myRepairHistory;
    private final Map<ScheduledTask, Set<LongTokenRange>> myTasks;
    private final int myTotalTokens;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    private VnodeOnDemandRepairJob(final Builder builder)
    {
        super(builder.configuration, builder.jmxProxyFactory, builder.repairConfiguration,
                builder.repairLockType, builder.onFinishedHook, builder.tableRepairMetrics, builder.ongoingJob,
                builder.currentNode, builder.myTimeBasedRunPolicy);
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory,
                "Repair history must be set");
        myTotalTokens = getOngoingJob().getTokens().size();
        myTimeBasedRunPolicy = builder.myTimeBasedRunPolicy;
        myTasks = createRepairTasks(getOngoingJob().getTokens(), getOngoingJob().getRepairedTokens(), builder.currentNode);
    }

    private Map<ScheduledTask, Set<LongTokenRange>> createRepairTasks(final Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRanges,
                                                                      final Set<LongTokenRange> repairedTokens,
                                                                      final Node currentNode)
    {
        // Check if repair is blocked by time-based run policy
        if (myTimeBasedRunPolicy != null && !myTimeBasedRunPolicy.shouldRun(getTableReference(), currentNode))
        {
            LOG.debug("Repair tasks creation skipped for {} - blocked by time-based run policy", getTableReference());
            return new ConcurrentHashMap<>();
        }

        Map<LongTokenRange, ImmutableSet<DriverNode>> remainingTokenRanges = filterRemainingTokenRanges(tokenRanges, repairedTokens);
        List<VnodeRepairState> vnodeRepairStates = createVnodeRepairStates(remainingTokenRanges);
        List<ReplicaRepairGroup> repairGroups = generateRepairGroups(vnodeRepairStates);

        return mapRepairGroupsToTasks(repairGroups, currentNode);
    }

    private Map<LongTokenRange, ImmutableSet<DriverNode>>
            filterRemainingTokenRanges(final Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRanges,
                                       final Set<LongTokenRange> repairedTokens)
    {
        if (repairedTokens.isEmpty())
        {
            return tokenRanges;
        }
        Map<LongTokenRange, ImmutableSet<DriverNode>> remainingTokenRanges = new HashMap<>(tokenRanges);
        repairedTokens.forEach(remainingTokenRanges::remove);
        return remainingTokenRanges;
    }

    private List<VnodeRepairState> createVnodeRepairStates(final Map<LongTokenRange, ImmutableSet<DriverNode>> remainingTokenRanges)
    {
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();
        for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> entry : remainingTokenRanges.entrySet())
        {
            vnodeRepairStates.add(new VnodeRepairState(entry.getKey(), entry.getValue(), -1));
        }
        return vnodeRepairStates;
    }

    private List<ReplicaRepairGroup> generateRepairGroups(final List<VnodeRepairState> vnodeRepairStates)
    {
        return VnodeRepairGroupFactory.INSTANCE.generateReplicaRepairGroups(vnodeRepairStates);
    }

    private Map<ScheduledTask, Set<LongTokenRange>> mapRepairGroupsToTasks(final List<ReplicaRepairGroup> repairGroups,
                                                                           final Node currentNode)
    {
        Map<ScheduledTask, Set<LongTokenRange>> taskMap = new ConcurrentHashMap<>();
        for (ReplicaRepairGroup replicaRepairGroup : repairGroups)
        {
            Set<LongTokenRange> groupTokenRange = new HashSet<>();
            replicaRepairGroup.iterator().forEachRemaining(groupTokenRange::add);
            ScheduledTask task = createScheduledTask(replicaRepairGroup, currentNode);
            taskMap.put(task, groupTokenRange);
        }
        return taskMap;
    }

    private ScheduledTask createScheduledTask(final ReplicaRepairGroup replicaRepairGroup, final Node currentNode)
    {
        return RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withRepairResourceFactory(getRepairLockType().getLockFactory())
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withRepairHistory(myRepairHistory)
                .withJobId(getJobId())
                .withNode(currentNode)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build(ScheduledJob.Priority.HIGHEST.getValue());
    }

    @Override
    public OnDemandRepairJobView getView()
    {
        return new OnDemandRepairJobView(
                getJobId(),
                getOngoingJob().getHostId(),
                getTableReference(),
                getStatus(),
                getProgress(),
                getOngoingJob().getCompletedTime(), getOngoingJob().getRepairType());
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return myTasks.keySet().iterator();
    }

    @Override
    public void postExecute(final boolean successful, final ScheduledTask task)
    {
        if (!successful)
        {
            LOG.error("Error running {}", task);
            setFailed(true);
        }
        else
        {
            Set<LongTokenRange> repairedTokenSet = myTasks.remove(task);
            getOngoingJob().finishRanges(repairedTokenSet);
        }

        super.postExecute(successful, task);
    }

    @Override
    public void finishJob()
    {
        UUID id = getJobId();
        getOnFinishedHook().accept(id);
        if (myTasks.isEmpty())
        {
            getOngoingJob().finishJob();
            LOG.info("Completed on demand repair: {}", id);
        }

        if (hasFailed())
        {
            getOngoingJob().failJob();
            LOG.error("Failed on demand repair: {}", id);
        }
        super.finishJob();
    }

    @Override
    public ScheduledJob.State getState()
    {
        if (hasFailed())
        {
            LOG.error("Repair job with id {} failed", getJobId());
            return ScheduledJob.State.FAILED;
        }
        if (getOngoingJob().hasTopologyChanged())
        {
            LOG.error("Repair job with id {} failed. Token ranges have changed since repair has was triggered",
                    getJobId());
            setFailed(true);
            return ScheduledJob.State.FAILED;
        }
        // Check if repair is blocked by time-based run policy
        if (myTimeBasedRunPolicy != null && !myTimeBasedRunPolicy.shouldRun(getTableReference(), getCurrentNode()))
        {
            LOG.debug("Repair job with id {} is blocked by time-based run policy", getJobId());
            return ScheduledJob.State.BLOCKED;
        }
        return myTasks.isEmpty() ? ScheduledJob.State.FINISHED : ScheduledJob.State.RUNNABLE;
    }

    public double getProgress()
    {
        if (myTotalTokens == 0)
        {
            LOG.debug("Total tokens for this job are 0");
            return 0;
        }

        OngoingJob ongoingJob = getOngoingJob();
        OngoingJob.Status state = ongoingJob.getStatus();
        int repairedTokens = ongoingJob.getRepairedTokens().size();
        return state == OngoingJob.Status.finished ? 1.0 : (double) repairedTokens / myTotalTokens;
    }

    @Override
    public String toString()
    {
        return String.format("Vnode On Demand Repair job of %s", getTableReference());
    }

    public static class Builder
    {
        private final ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private DistributedJmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private Consumer<UUID> onFinishedHook = table ->
        {
        };
        private RepairHistory repairHistory;
        private OngoingJob ongoingJob;
        private Node currentNode;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        public final Builder withNode(final Node node)
        {
            this.currentNode = node;
            return this;
        }

        public final Builder withJmxProxyFactory(final DistributedJmxProxyFactory aJMXProxyFactory)
        {
            this.jmxProxyFactory = aJMXProxyFactory;
            return this;
        }

        public final Builder withTableRepairMetrics(final TableRepairMetrics theTableRepairMetrics)
        {
            this.tableRepairMetrics = theTableRepairMetrics;
            return this;
        }

        public final Builder withRepairLockType(final RepairLockType aRepairLockType)
        {
            this.repairLockType = aRepairLockType;
            return this;
        }

        public final Builder withOnFinished(final Consumer<UUID> theOnFinishedHook)
        {
            this.onFinishedHook = theOnFinishedHook;
            return this;
        }

        public final Builder withRepairConfiguration(final RepairConfiguration aRepairConfiguration)
        {
            this.repairConfiguration = aRepairConfiguration;
            return this;
        }

        public final Builder withRepairHistory(final RepairHistory aRepairHistory)
        {
            this.repairHistory = aRepairHistory;
            return this;
        }

        public final Builder withOngoingJob(final OngoingJob anOngoingJob)
        {
            this.ongoingJob = anOngoingJob;
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

        public final VnodeOnDemandRepairJob build()
        {
            return new VnodeOnDemandRepairJob(this);
        }
    }
}
