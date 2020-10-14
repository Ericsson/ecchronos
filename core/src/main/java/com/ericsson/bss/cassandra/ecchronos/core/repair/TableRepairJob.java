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
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this node if all the ranges this node
 * is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
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

    TableRepairJob(Builder builder)
    {
        super(builder.configuration);

        myTableReference = Preconditions.checkNotNull(builder.tableReference, "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(builder.jmxProxyFactory, "JMX Proxy Factory must be set");
        myRepairState = Preconditions.checkNotNull(builder.repairState, "Repair state must be set");
        myTableRepairMetrics = Preconditions
                .checkNotNull(builder.tableRepairMetrics, "Table repair metrics must be set");
        myRepairConfiguration = Preconditions
                .checkNotNull(builder.repairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(builder.repairLockType, "Repair lock type must be set");
        myTableStorageStates = Preconditions
                .checkNotNull(builder.tableStorageStates, "Table storage states must be set");
        myRepairPolicies = Preconditions.checkNotNull(builder.repairPolicies, "Repair policies cannot be null");
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
        long now = System.currentTimeMillis();
        return new RepairJobView(getId(), myTableReference, myRepairConfiguration, myRepairState.getSnapshot(), getStatus(now), getProgress(now));
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

    private RepairJobView.Status getStatus(long timestamp)
    {
        long repairedAt = myRepairState.getSnapshot().lastRepairedAt();
        long msSinceLastRepair = timestamp - repairedAt;
        RepairConfiguration config = myRepairConfiguration;

        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return RepairJobView.Status.ERROR;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return RepairJobView.Status.WARNING;
        }
        if (msSinceLastRepair >= config.getRepairIntervalInMs())
        {
            return RepairJobView.Status.IN_QUEUE;
        }
        return RepairJobView.Status.COMPLETED;
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
                RepairGroup.Builder builder = RepairGroup.newBuilder()
                        .withTableReference(myTableReference)
                        .withRepairConfiguration(myRepairConfiguration)
                        .withReplicaRepairGroup(replicaRepairGroup)
                        .withJmxProxyFactory(myJmxProxyFactory)
                        .withTableRepairMetrics(myTableRepairMetrics)
                        .withRepairResourceFactory(myRepairLockType.getLockFactory())
                        .withRepairLockFactory(repairLockFactory)
                        .withTokensPerRepair(tokensPerRepair)
                        .withRepairPolicies(myRepairPolicies);

                taskList.add(builder.build(getRealPriority()));
            }

            return taskList.iterator();
        }
        else
        {
            return Collections.emptyIterator();
        }
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

        super.postExecute(successful, task);
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return myRepairState.getSnapshot().lastRepairedAt();
    }

    @Override
    public boolean runnable()
    {
        if (super.runnable())
        {
            try
            {
                myRepairState.update();
            } catch (Exception e)
            {
                LOG.warn("Unable to check repair history, {}", this, e);
            }
        }

        return myRepairState.getSnapshot().canRepair() && super.runnable();
    }

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    private BigInteger getTokensPerRepair(VnodeRepairStates vnodeRepairStates)
    {
        BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;

        if (myRepairConfiguration.getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            BigInteger tableSizeInBytes = BigInteger.valueOf(myTableStorageStates.getDataSize(myTableReference));

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

    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(7, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private RepairState repairState;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private TableStorageStates tableStorageStates;
        private final List<TableRepairPolicy> repairPolicies = new ArrayList<>();

        public Builder withConfiguration(Configuration configuration)
        {
            this.configuration = configuration;
            return this;
        }

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

        public Builder withRepairState(RepairState repairState)
        {
            this.repairState = repairState;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            this.repairLockType = repairLockType;
            return this;
        }

        public Builder withTableStorageStates(TableStorageStates tableStorageStates)
        {
            this.tableStorageStates = tableStorageStates;
            return this;
        }

        public Builder withRepairPolices(Collection<TableRepairPolicy> tableRepairPolicies)
        {
            this.repairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        public TableRepairJob build()
        {
            return new TableRepairJob(this);
        }
    }
}
