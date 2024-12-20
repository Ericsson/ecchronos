/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairGroupFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.base.Preconditions;
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
 * A Job that will schedule and run vnode repair on one table once. It creates {@link VnodeRepairTask VnodeRepairTasks}
 * to fully repair the table for the current node. Once all {@link VnodeRepairTask VnodeRepairTasks} are completed,
 * the repair is finished and the job will be descheduled.
 */
public final class VnodeOnDemandRepairJob extends OnDemandRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(VnodeOnDemandRepairJob.class);
    private final RepairHistory myRepairHistory;
    private final Map<ScheduledTask, Set<LongTokenRange>> myTasks;
    private final int myTotalTokens;

    private VnodeOnDemandRepairJob(final Builder builder)
    {
        super(builder.configuration, builder.jmxProxyFactory, builder.repairConfiguration,
                builder.repairLockType, builder.onFinishedHook, builder.tableRepairMetrics, builder.ongoingJob);
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory,
                "Repair history must be set");
        myTotalTokens = getOngoingJob().getTokens().size();
        myTasks = createRepairTasks(getOngoingJob().getTokens(), getOngoingJob().getRepairedTokens());
    }

    private Map<ScheduledTask, Set<LongTokenRange>> createRepairTasks(
            final Map<LongTokenRange, Set<DriverNode>> tokenRanges,
            final Set<LongTokenRange> repairedTokens)
    {
        Map<ScheduledTask, Set<LongTokenRange>> taskMap = new ConcurrentHashMap<>();
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();
        Map<LongTokenRange, Set<DriverNode>> remainingTokenRanges;

        if (repairedTokens.isEmpty())
        {
            remainingTokenRanges = tokenRanges;
        }
        else
        {
            remainingTokenRanges = new HashMap<>(tokenRanges);
            repairedTokens.iterator().forEachRemaining(remainingTokenRanges::remove);
        }

        for (Map.Entry<LongTokenRange, Set<DriverNode>> entry : remainingTokenRanges.entrySet())
        {
            LongTokenRange longTokenRange = entry.getKey();
            Set<DriverNode> replicas = entry.getValue();
            vnodeRepairStates.add(new VnodeRepairState(longTokenRange, replicas, -1));
        }

        List<ReplicaRepairGroup> repairGroups = VnodeRepairGroupFactory.INSTANCE
                .generateReplicaRepairGroups(vnodeRepairStates);

        for (ReplicaRepairGroup replicaRepairGroup : repairGroups)
        {
            Set<LongTokenRange> groupTokenRange = new HashSet<>();
            replicaRepairGroup.iterator().forEachRemaining(groupTokenRange::add);

            taskMap.put(RepairGroup.newBuilder()
                    .withTableReference(getTableReference())
                    .withRepairConfiguration(getRepairConfiguration())
                    .withReplicaRepairGroup(replicaRepairGroup)
                    .withJmxProxyFactory(getJmxProxyFactory())
                    .withTableRepairMetrics(getTableRepairMetrics())
                    .withRepairResourceFactory(getRepairLockType().getLockFactory())
                    .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                    .withRepairHistory(myRepairHistory)
                    .withJobId(getId())
                    .build(ScheduledJob.Priority.HIGHEST.getValue()), groupTokenRange);
        }
        return taskMap;
    }

    @Override
    public OnDemandRepairJobView getView()
    {
        return new OnDemandRepairJobView(
                getId(),
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
        UUID id = getId();
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
            LOG.error("Repair job with id {} failed", getId());
            return ScheduledJob.State.FAILED;
        }
        if (getOngoingJob().hasTopologyChanged())
        {
            LOG.error("Repair job with id {} failed. Token ranges have changed since repair has was triggered",
                    getId());
            setFailed(true);
            return ScheduledJob.State.FAILED;
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
        Status state = ongoingJob.getStatus();
        int  repairedTokens = ongoingJob.getRepairedTokens().size();
        return state == Status.finished ? 1 : (double) repairedTokens / myTotalTokens;
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
        private JmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private Consumer<UUID> onFinishedHook = table ->
        {
        };
        private RepairHistory repairHistory;
        private OngoingJob ongoingJob;

        public final Builder withJmxProxyFactory(final JmxProxyFactory aJMXProxyFactory)
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

        public final VnodeOnDemandRepairJob build()
        {
            return new VnodeOnDemandRepairJob(this);
        }
    }
}
