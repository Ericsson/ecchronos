/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * A Job that will schedule and run repair on one table once. It creates {@link RepairTask RepairTasks} to fully repair
 * the table for the current node. Once all {@link RepairTask RepairTasks} are completed, the repair is finished and the job will be
 * descheduled.
 */
public class OnDemandRepairJob extends ScheduledJob
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandRepairJob.class);

    private static final RepairLockFactory repairLockFactory = new RepairLockFactoryImpl();

    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final Consumer<UUID> myOnFinishedHook;
    private final RepairHistory myRepairHistory;

    private final TableRepairMetrics myTableRepairMetrics;

    private final Map<ScheduledTask, Set<LongTokenRange>> myTasks;

    private final int myTotalTasks;

    private boolean failed = false;

    private final OngoingJob myOngoingJob;

    private OnDemandRepairJob(OnDemandRepairJob.Builder builder)
    {
        super(builder.configuration, builder.ongoingJob.getJobId());

        myOngoingJob = Preconditions.checkNotNull(builder.ongoingJob, "Ongoing job must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(builder.jmxProxyFactory, "JMX Proxy Factory must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(builder.tableRepairMetrics, "Table repair metrics must be set");
        myRepairConfiguration = Preconditions.checkNotNull(builder.repairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(builder.repairLockType, "Repair lock type must be set");
        myOnFinishedHook = Preconditions.checkNotNull(builder.onFinishedHook, "On finished hook must be set");
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory, "Repair history must be set");

        myTasks = createRepairTasks(myOngoingJob.getTokens(), myOngoingJob.getRepairedTokens());
        myTotalTasks = myTasks.size();
    }

    private Map<ScheduledTask, Set<LongTokenRange>> createRepairTasks(Map<LongTokenRange, ImmutableSet<Node>> tokenRanges, Set<LongTokenRange> repairedTokens)
    {
        Map<ScheduledTask, Set<LongTokenRange>> taskMap = new ConcurrentHashMap<>();
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();
        Map<LongTokenRange, ImmutableSet<Node>> remainingTokenRanges;

        if(repairedTokens.isEmpty())
        {
            remainingTokenRanges = tokenRanges;
        }
        else
        {
            remainingTokenRanges = new HashMap<>(tokenRanges);
            repairedTokens.iterator().forEachRemaining(t -> remainingTokenRanges.remove(t));
        }

        for (Map.Entry<LongTokenRange, ImmutableSet<Node>> entry : remainingTokenRanges.entrySet())
        {
            LongTokenRange longTokenRange = entry.getKey();
            ImmutableSet<Node> replicas = entry.getValue();
            vnodeRepairStates.add(new VnodeRepairState(longTokenRange, replicas, -1));
        }

        List<ReplicaRepairGroup> repairGroups = VnodeRepairGroupFactory.INSTANCE
                .generateReplicaRepairGroups(vnodeRepairStates);

        for (ReplicaRepairGroup replicaRepairGroup : repairGroups)
        {
            Set<LongTokenRange> groupTokenRange = new HashSet<>();
            replicaRepairGroup.iterator().forEachRemaining(t -> groupTokenRange.add(t));

            taskMap.put(RepairGroup.newBuilder()
                    .withTableReference(myOngoingJob.getTableReference())
                    .withRepairConfiguration(myRepairConfiguration)
                    .withReplicaRepairGroup(replicaRepairGroup)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withTableRepairMetrics(myTableRepairMetrics)
                    .withRepairResourceFactory(myRepairLockType.getLockFactory())
                    .withRepairLockFactory(repairLockFactory)
                    .withRepairHistory(myRepairHistory)
                    .withJobId(getId())
                    .build(Priority.HIGHEST.getValue()), groupTokenRange);
        }
        return taskMap;
    }

    public TableReference getTableReference()
    {
        return myOngoingJob.getTableReference();
    }

    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    public RepairJobView getView()
    {
        return new OnDemandRepairJobView(
                getId(),
                myOngoingJob.getTableReference(),
                myRepairConfiguration,
                getStatus(),
                getProgress(),
                myOngoingJob.getCompletedTime());
    }

    private RepairJobView.Status getStatus()
    {
        if (failed || myOngoingJob.getStatus() == Status.failed)
        {
            return RepairJobView.Status.ERROR;
        }
        else if(myOngoingJob.getStatus() == Status.finished)
        {
            return RepairJobView.Status.COMPLETED;
        }
        return RepairJobView.Status.IN_QUEUE;
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return myTasks.keySet().iterator();
    }

    @Override
    public void postExecute(boolean successful, ScheduledTask task)
    {
        if (!successful)
        {
            LOG.error("Error running {}", task);
            failed = true;
        }
        else
        {
            Set<LongTokenRange> repairedTokenSet = myTasks.remove(task);
            myOngoingJob.finishRanges(repairedTokenSet);
        }

        super.postExecute(successful, task);
    }

    @Override
    public void finishJob()
    {
        UUID id = getId();
        if (myTasks.isEmpty())
        {
            myOnFinishedHook.accept(id);
            myOngoingJob.finishJob();
            LOG.info("Completed On Demand Repair: {}", id);
        }

        if (failed)
        {
            myOnFinishedHook.accept(id);
            myOngoingJob.failJob();
            LOG.error("Failed On Demand Repair: {}", id);
        }
        super.finishJob();
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return -1;
    }

    @Override
    public boolean runnable()
    {
        return getState().equals(State.RUNNABLE);
    }

    @Override
    public State getState()
    {
        if (failed)
        {
            LOG.error("Repair job with id {} failed", getId());
            return State.FAILED;
        }
        if (myOngoingJob.hasTopologyChanged())
        {
            LOG.error("Repair job with id {} failed, token Ranges have changed since repair has was triggered", getId());
            failed = true;
            return State.FAILED;
        }
        return myTasks.isEmpty() ? State.FINISHED : State.RUNNABLE;
    }

    public double getProgress()
    {
        int finishedTasks = myTotalTasks - myTasks.size();
        return myTotalTasks == 0 ? 1 : (double) finishedTasks / myTotalTasks;
    }

    @Override
    public String toString()
    {
        return String.format("On Demand Repair job of %s", myOngoingJob.getTableReference());
    }

    public static class Builder
    {
        private final Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private JmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private Consumer<UUID> onFinishedHook = table -> {
        };
        private RepairHistory repairHistory;
        private OngoingJob ongoingJob;

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            this.jmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            this.repairLockType = repairLockType;
            return this;
        }

        public Builder withOnFinished(Consumer<UUID> onFinishedHook)
        {
            this.onFinishedHook = onFinishedHook;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withRepairHistory(RepairHistory repairHistory)
        {
            this.repairHistory = repairHistory;
            return this;
        }

        public Builder withOngoingJob(OngoingJob ongoingJob)
        {
            this.ongoingJob = ongoingJob;
            return this;
        }

        public OnDemandRepairJob build()
        {
            return new OnDemandRepairJob(this);
        }
    }
}
