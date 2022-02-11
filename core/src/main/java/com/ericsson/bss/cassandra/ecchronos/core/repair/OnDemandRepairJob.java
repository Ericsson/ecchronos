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

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
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

    private boolean failed = false;

    private final OngoingJob myOngoingJob;
    private final TableStorageStates myTableStorageStates;
    private final RepairState myRepairState;

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
        myTableStorageStates = Preconditions
                .checkNotNull(builder.tableStorageStates, "Table storage states must be set");
        myRepairState = Preconditions.checkNotNull(builder.repairState, "Repair state must be set");
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
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();
        if (repairStateSnapshot.canRepair())
        {
            List<ScheduledTask> taskList = new ArrayList<>();
            BigInteger tokensPerRepair = getTokensPerRepair(repairStateSnapshot.getVnodeRepairStates());
            for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                RepairGroup repairGroup = RepairGroup.newBuilder()
                        .withTableReference(getTableReference())
                        .withRepairConfiguration(myRepairConfiguration)
                        .withReplicaRepairGroup(replicaRepairGroup)
                        .withJmxProxyFactory(myJmxProxyFactory)
                        .withTableRepairMetrics(myTableRepairMetrics)
                        .withRepairResourceFactory(myRepairLockType.getLockFactory())
                        .withRepairLockFactory(repairLockFactory)
                        .withTokensPerRepair(tokensPerRepair)
                        .withRepairHistory(myRepairHistory)
                        .withJobId(getId())
                        .build(Priority.HIGHEST.getValue());
                taskList.add(repairGroup);
            }
            return taskList.iterator();
        }
        return Collections.emptyIterator();
    }

    private BigInteger getTokensPerRepair(VnodeRepairStates vnodeRepairStates)
    {
        BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;
        if (myRepairConfiguration.getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            BigInteger tableSizeInBytes = BigInteger.valueOf(myTableStorageStates.getDataSize(getTableReference()));
            if (!BigInteger.ZERO.equals(tableSizeInBytes))
            {
                BigInteger fullRangeSize = vnodeRepairStates.getVnodeRepairStates().stream()
                        .map(VnodeRepairState::getTokenRange)
                        .map(LongTokenRange::rangeSize)
                        .reduce(BigInteger.ZERO, BigInteger::add);
                BigInteger targetSizeInBytes = BigInteger.valueOf(myRepairConfiguration.getTargetRepairSizeInBytes());
                BigInteger targetRepairs = tableSizeInBytes.divide(targetSizeInBytes);
                tokensPerRepair = fullRangeSize.divide(targetRepairs);
            }
        }
        return tokensPerRepair;
    }

    @Override
    public void postExecute(boolean successful, ScheduledTask task)
    {
        try
        {
            myRepairState.update();
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }
        if (!successful)
        {
            LOG.error("Error running {}", task);
            failed = true;
        }
        super.postExecute(successful, task);
    }

    @Override
    public void finishJob()
    {
        UUID id = getId();
        if (getProgress() == 1.0)
        {
            myOnFinishedHook.accept(id);
            myOngoingJob.finishJob();
            LOG.info("Completed On Demand Repair: {}", id);
        }

        if (failed)
        {
            myOnFinishedHook.accept(getId());
            myOngoingJob.failJob();
            LOG.error("Failed On Demand Repair: {}", id);
        }
        super.finishJob();
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return myOngoingJob.getCompletedTime() != -1 ? myOngoingJob.getCompletedTime() : myRepairState.getSnapshot().lastCompletedAt();
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
        return getProgress() == 1.0 ? State.FINISHED : State.RUNNABLE;
    }

    public double getProgress()
    {
        if (myOngoingJob.getCompletedTime() != -1)
        {
            return 1.0;
        }
        long interval = myRepairConfiguration.getRepairIntervalInMs();
        Collection<VnodeRepairState> states = myRepairState.getSnapshot().getVnodeRepairStates().getVnodeRepairStates();
        long nRepaired = states.stream()
                .filter(isRepaired(myOngoingJob.getStartedTime(), interval))
                .count();
        return states.isEmpty()
                ? 0
                : (double) nRepaired / states.size();
    }

    private Predicate<VnodeRepairState> isRepaired(long timestamp, long interval)
    {
        return state -> timestamp - state.lastRepairedAt() <= interval;
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
        private RepairConfiguration repairConfiguration = RepairConfiguration.DISABLED;
        private RepairLockType repairLockType;
        private Consumer<UUID> onFinishedHook = table -> {
        };
        private RepairHistory repairHistory;
        private OngoingJob ongoingJob;
        private RepairState repairState;
        private TableStorageStates tableStorageStates;

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

        public Builder withRepairState(RepairState repairState)
        {
            this.repairState = repairState;
            return this;
        }

        public Builder withTableStorageStates(TableStorageStates tableStorageStates)
        {
            this.tableStorageStates = tableStorageStates;
            return this;
        }

        public OnDemandRepairJob build()
        {
            return new OnDemandRepairJob(this);
        }
    }
}
