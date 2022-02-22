/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this node if all the ranges this node
 * is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
@SuppressWarnings("PMD.TooManyFields")
public class TableRepairJob extends ScheduledJob
{
    private static final Logger LOG = LoggerFactory.getLogger(TableRepairJob.class);

    private static final RepairLockFactory repairLockFactory = new RepairLockFactoryImpl();

    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairState myRepairState;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableRepairMetrics myTableRepairMetrics;
    private final TableStorageStates myTableStorageStates;
    private final RepairHistory myRepairHistory;
    private final Consumer<TableRepairJob> myOnFinishedHook;
    private final RepairStatus myRepairStatus;
    private final String myTriggeredBy;
    private final UUID myRepairId;
    private State myCurrentState;
    private long myStartedAt;
    private List<ScheduledTask> myTasks;
    private int myTotalTasks;

    TableRepairJob(Builder builder)
    {
        super(builder.myConfiguration, builder.myJobId);
        myTableReference = builder.myTableReference;
        myJmxProxyFactory = Preconditions.checkNotNull(builder.myJmxProxyFactory, "JMX Proxy Factory must be set");
        myRepairState = Preconditions.checkNotNull(builder.myRepairState, "Repair state must be set");
        myTableRepairMetrics = Preconditions
                .checkNotNull(builder.myTableRepairMetrics, "Table repair metrics must be set");
        myRepairConfiguration = Preconditions
                .checkNotNull(builder.myRepairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(builder.myRepairLockType, "Repair lock type must be set");
        myTableStorageStates = Preconditions
                .checkNotNull(builder.myTableStorageStates, "Table storage states must be set");
        myRepairPolicies = Preconditions.checkNotNull(builder.myRepairPolicies, "Repair policies cannot be null");
        myRepairHistory = Preconditions.checkNotNull(builder.myRepairHistory, "Repair history must be set");
        myOnFinishedHook = Preconditions.checkNotNull(builder.myOnFinishedHook, "On finished hook must be set");
        myRepairStatus = Preconditions.checkNotNull(builder.myRepairStatus, "Repair status must be set");
        myTriggeredBy = Preconditions.checkNotNull(builder.myTriggeredBy, "Triggered by must be set");
        myRepairId = Preconditions.checkNotNull(builder.myRepairId, "Repair id must be set");
        myStartedAt = builder.myStartedAt;
        myCurrentState = State.PARKED;
    }

    public TableReference getTableReference()
    {
        return myTableReference;
    }

    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return new ArrayList<>(myTasks).iterator();
    }

    public boolean isOnDemand()
    {
        return "user".equals(myTriggeredBy);
    }

    @Override
    public void postExecute(boolean successful, ScheduledTask task)
    {
        super.postExecute(successful, task);
        if(successful)
        {
            myTasks.remove(task);
            myRepairStatus.updateWithRemainingTasks(myRepairId, myTasks.size());
        }
    }

    @Override
    public boolean runnable()
    {
        if(super.runnable())
        {
            try
            {
                myRepairState.update();
            }
            catch(Exception e)
            {
                LOG.warn("Unable to check repair history, {}", this, e);
            }
        }
        LOG.debug("Can repair: {} super: {}", myRepairState.getSnapshot().canRepair(), super.runnable());
        return myRepairState.getSnapshot().canRepair() && super.runnable();
    }

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    @Override
    public State getState()
    {
        State newState = super.getState();
        if(myCurrentState.equals(State.RUNNABLE) && isFinished())
        {
            newState = State.FINISHED;
        }
        if(newState.equals(State.RUNNABLE) && myCurrentState.equals(State.PARKED))
        {
            if(myStartedAt == -1L)
            {
                myStartedAt = System.currentTimeMillis();
            }
            myTasks = getTasks();
            myTotalTasks = myTasks.size();
            LOG.debug("Starting running {}, total tasks: {}", getId(), myTotalTasks);
            myRepairStatus.startRepair(myRepairId, myTableReference, myTriggeredBy, myTotalTasks, myStartedAt);
        }
        myCurrentState = newState;
        LOG.debug("Schedule: {} having state {}", getId(), myCurrentState);
        return newState;
    }

    private List<ScheduledTask> getTasks()
    {
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();
        List<ScheduledTask> taskList = new ArrayList<>();
        if(repairStateSnapshot.canRepair())
        {
            BigInteger tokensPerRepair = getTokensPerRepair(repairStateSnapshot.getVnodeRepairStates());
            for(ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                RepairGroup.Builder builder = RepairGroup.newBuilder()
                        .withTableReference(myTableReference)
                        .withRepairConfiguration(myRepairConfiguration)
                        .withReplicaRepairGroup(replicaRepairGroup)
                        .withJmxProxyFactory(myJmxProxyFactory)
                        .withTableRepairMetrics(myTableRepairMetrics)
                        .withRepairResourceFactory(myRepairLockType.getLockFactory())
                        .withRepairLockFactory(repairLockFactory)
                        .withTokensPerRepair(tokensPerRepair)
                        .withRepairPolicies(myRepairPolicies)
                        .withRepairHistory(myRepairHistory)
                        .withJobId(getId());
                int priority = getRealPriority();
                taskList.add(builder.build(priority));
            }
        }
        return taskList;
    }

    private BigInteger getTokensPerRepair(VnodeRepairStates vnodeRepairStates)
    {
        BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;
        if(myRepairConfiguration.getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            BigInteger tableSizeInBytes = BigInteger.valueOf(myTableStorageStates.getDataSize(myTableReference));
            if(!BigInteger.ZERO.equals(tableSizeInBytes))
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

    public ScheduleView getView()
    {
        return new ScheduleView(getId(), myTableReference, myRepairConfiguration, myRepairState.getSnapshot(),
                getStatus(), getProgress(), getLastSuccessfulRun(),
                getLastSuccessfulRun() + myRepairConfiguration.getRepairIntervalInMs());
    }

    public ScheduledRepairJobView getOldView()
    {
        long now = System.currentTimeMillis();
        return new ScheduledRepairJobView(getId(), myTableReference, myRepairConfiguration, myRepairState.getSnapshot(),
                getStatus(now), getProgress(now));

    }

    private RepairJobView.Status getStatus(long timestamp)
    {
        long repairedAt = myRepairState.getSnapshot().lastCompletedAt();
        long msSinceLastRepair = timestamp - repairedAt;
        RepairConfiguration config = myRepairConfiguration;

        if(msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return RepairJobView.Status.ERROR;
        }
        if(msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return RepairJobView.Status.WARNING;
        }
        if(msSinceLastRepair >= config.getRepairIntervalInMs())
        {
            return RepairJobView.Status.IN_QUEUE;
        }
        return RepairJobView.Status.COMPLETED;
    }

    private double getProgress(long timestamp)
    {
        long interval = myRepairConfiguration.getRepairIntervalInMs();
        Collection<VnodeRepairState> states = myRepairState.getSnapshot().getVnodeRepairStates().getVnodeRepairStates();

        long nRepaired = states.stream()
                .filter(isRepaired(timestamp, interval))
                .count();

        return states.isEmpty()
                ? 0
                : (double) nRepaired / states.size();
    }

    private Predicate<VnodeRepairState> isRepaired(long timestamp, long interval)
    {
        return state -> timestamp - state.lastRepairedAt() <= interval;
    }

    private boolean isFinished()
    {
        return getProgress() == 1.0;
    }

    private double getProgress()
    {
        if(!myCurrentState.equals(State.RUNNABLE))
        {
            return 0.0;
        }
        int finishedTasks = myTotalTasks - myTasks.size();
        return myTotalTasks == 0 ? 1 : (double) finishedTasks / myTotalTasks;
    }

    private ScheduleView.Status getStatus()
    {
        long repairedAt = myRepairState.getSnapshot().lastCompletedAt();
        long msSinceLastRepair = System.currentTimeMillis() - repairedAt;
        RepairConfiguration config = myRepairConfiguration;

        if(msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return ScheduleView.Status.ERROR;
        }
        if(msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return ScheduleView.Status.WARNING;
        }
        if(myCurrentState.equals(State.FINISHED))
        {
            return ScheduleView.Status.COMPLETED;
        }
        return myCurrentState.equals(State.PARKED) ? ScheduleView.Status.WAITING : ScheduleView.Status.RUNNING;
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return myRepairState.getSnapshot().lastCompletedAt();
    }

    @Override
    public void finishJob()
    {
        myRepairStatus.finishRepair(myRepairId);
        myOnFinishedHook.accept(this);
    }

    public static class Builder
    {
        Configuration myConfiguration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(7, TimeUnit.DAYS)
                .build();
        private TableReference myTableReference;
        private JmxProxyFactory myJmxProxyFactory;
        private RepairState myRepairState;
        private TableRepairMetrics myTableRepairMetrics = null;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType myRepairLockType;
        private TableStorageStates myTableStorageStates;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private RepairHistory myRepairHistory;
        private Consumer<TableRepairJob> myOnFinishedHook = table ->
        {
        };
        private RepairStatus myRepairStatus;
        private UUID myRepairId;
        private UUID myJobId;
        private long myStartedAt = -1L;
        private String myTriggeredBy;

        public Builder withConfiguration(Configuration configuration)
        {
            myConfiguration = configuration;
            return this;
        }

        public Builder withTableReference(TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withRepairState(RepairState repairState)
        {
            myRepairState = repairState;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        public Builder withTableStorageStates(TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        public Builder withRepairPolices(Collection<TableRepairPolicy> tableRepairPolicies)
        {
            myRepairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        public Builder withRepairHistory(RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        public Builder withOnFinished(Consumer<TableRepairJob> onFinishedHook)
        {
            myOnFinishedHook = onFinishedHook;
            return this;
        }

        public Builder withRepairStatus(RepairStatus repairStatus)
        {
            myRepairStatus = repairStatus;
            return this;
        }

        public Builder withRepairId(UUID id)
        {
            myRepairId = id;
            return this;
        }

        public Builder withJobId(UUID id)
        {
            myJobId = id;
            return this;
        }

        public Builder withStartedAt(long startedAt)
        {
            myStartedAt = startedAt;
            return this;
        }

        public Builder withTriggeredBy(String triggeredBy)
        {
            myTriggeredBy = triggeredBy;
            return this;
        }

        public TableRepairJob build()
        {
            Preconditions.checkNotNull(myTableReference, "Table reference must be set");
            return new TableRepairJob(this);
        }
    }
}
