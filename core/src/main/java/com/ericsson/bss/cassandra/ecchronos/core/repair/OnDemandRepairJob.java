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

import com.datastax.driver.core.UDTValue;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus.OngoingJob;
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

    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final ReplicationState myReplicationState;
    private final Consumer<UUID> myOnFinishedHook;
    private final RepairHistory myRepairHistory;

    private final TableRepairMetrics myTableRepairMetrics;

    private final Map<ScheduledTask, Set<LongTokenRange>> myTasks;

    private final Map<LongTokenRange, ImmutableSet<Node>> myTokens;

    private final Integer myTokenHash;

    private final Set<UDTValue> myRepairedTokens;

    private final int myTotalTasks;

    private boolean failed = false;

    private final OnDemandStatus myOnDemandStatus;

    public OnDemandRepairJob(OnDemandRepairJob.Builder builder)
    {
        super(builder.configuration, builder.ongoingJob == null ? null : builder.ongoingJob.getJobId());

        myTableReference = Preconditions.checkNotNull(builder.ongoingJob == null ? builder.tableReference : builder.ongoingJob.getTableReference(), "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(builder.jmxProxyFactory, "JMX Proxy Factory must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(builder.tableRepairMetrics, "Table repair metrics must be set");
        myRepairConfiguration = Preconditions.checkNotNull(builder.repairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(builder.repairLockType, "Repair lock type must be set");
        myReplicationState = Preconditions.checkNotNull(builder.replicationState, "Replication state must be set");
        myOnFinishedHook = Preconditions.checkNotNull(builder.onFinishedHook, "On finished hook must be set");
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory, "Repair history must be set");
        myOnDemandStatus = Preconditions.checkNotNull(builder.onDemandStatus, "OnDemandStatus must be set");

        myTokens = myReplicationState
                .getTokenRangeToReplicas(myTableReference);
        if (builder.ongoingJob != null)
        {
            myTokenHash = builder.ongoingJob.getTokenMapHash();
            myRepairedTokens = builder.ongoingJob.getRepiaredTokens();
        }
        else
        {
            myTokenHash = null;
            myRepairedTokens = new HashSet<>();
        }
        myTasks = createRepairTasks(myTokens, myRepairedTokens);
        myTotalTasks = myTasks.size();

        if(builder.ongoingJob == null)
        {
            myOnDemandStatus.addNewJob(getId(), myTableReference, myTokens.hashCode());
        }
    }

    private Map<ScheduledTask, Set<LongTokenRange>> createRepairTasks(Map<LongTokenRange, ImmutableSet<Node>> tokenRanges, Set<UDTValue> repairedTokens)
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
            Set<LongTokenRange> tokensToRemove = new HashSet<>();
            repairedTokens.iterator().forEachRemaining(t -> tokensToRemove.add(new LongTokenRange(myOnDemandStatus.getStartTokenFrom(t), myOnDemandStatus.getEndTokenFrom(t))));
            remainingTokenRanges = new HashMap<>(tokenRanges);
            tokensToRemove.iterator().forEachRemaining(t -> remainingTokenRanges.remove(t));
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
                    .withTableReference(myTableReference)
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
        return myTableReference;
    }

    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    public RepairJobView getView()
    {
        return new RepairJobView(getId(), myTableReference, myRepairConfiguration, null, getStatus(), getProgress());
    }

    private RepairJobView.Status getStatus()
    {
        return failed ? RepairJobView.Status.ERROR : RepairJobView.Status.IN_QUEUE;
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

        Set<LongTokenRange> tokenSet = myTasks.remove(task);
        tokenSet.forEach(t -> myRepairedTokens.add(myOnDemandStatus.createUDTTokenRangeValue(t.start, t.end)));
        myOnDemandStatus.updateJob(getId(), myRepairedTokens);

        if (myTasks.isEmpty())
        {
            myOnFinishedHook.accept(getId());
            myOnDemandStatus.finishJob(getId());
        }

        if(failed)
        {
            myOnDemandStatus.failJob(getId());
        }

        super.postExecute(successful, task);
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
        if (!myTokens.equals(myReplicationState.getTokenRangeToReplicas(myTableReference))
                || (myTokenHash != null && myTokenHash != myTokens.hashCode()))
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
        return myTotalTasks == 0 ? 0 : (double) finishedTasks / myTotalTasks;
    }

    @Override
    public String toString()
    {
        return String.format("On Demand Repair job of %s", myTableReference);
    }

    public static class Builder
    {
        private final Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private ReplicationState replicationState;
        private Consumer<UUID> onFinishedHook = table -> {
        };
        private RepairHistory repairHistory;
        private OnDemandStatus onDemandStatus;
        private OngoingJob ongoingJob = null;

        public Builder withTableReference(TableReference tableReference)
        {
            this.tableReference = tableReference;
            return this;
        }

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

        public Builder withReplicationState(ReplicationState replicationState)
        {
            this.replicationState = replicationState;
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

        public Builder withOnDemandStatus(OnDemandStatus ondemandStatus)
        {
            this.onDemandStatus = ondemandStatus;
            return this;
        }

        public Builder withOngoingJob(OngoingJob ongoingJob)
        {
            this.ongoingJob  = ongoingJob;
            return this;
        }

        public OnDemandRepairJob build()
        {
            return new OnDemandRepairJob(this);
        }
    }
}
