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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this
 * node if all the ranges this node is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
public class TableRepairJob extends ScheduledJob
{
    private static final int DAYS_IN_A_WEEK = 7;

    private static final Logger LOG = LoggerFactory.getLogger(TableRepairJob.class);

    private static final RepairLockFactory REPAIR_LOCK_FACTORY = new RepairLockFactoryImpl();

    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairState myRepairState;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final List<TableRepairPolicy> myRepairPolicies;

    private final TableRepairMetrics myTableRepairMetrics;
    private final TableStorageStates myTableStorageStates;
    private final RepairHistory myRepairHistory;

    TableRepairJob(final Builder builder)
    {
        super(builder.configuration, builder.tableReference.getId());

        myTableReference = builder.tableReference;
        myJmxProxyFactory = Preconditions.checkNotNull(builder.jmxProxyFactory,
                "JMX Proxy Factory must be set");
        myRepairState = Preconditions.checkNotNull(builder.repairState,
                "Repair state must be set");
        myTableRepairMetrics = Preconditions
                .checkNotNull(builder.tableRepairMetrics,
                        "Table repair metrics must be set");
        myRepairConfiguration = Preconditions
                .checkNotNull(builder.repairConfiguration,
                        "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(builder.repairLockType,
                "Repair lock type must be set");
        myTableStorageStates = Preconditions
                .checkNotNull(builder.tableStorageStates,
                        "Table storage states must be set");
        myRepairPolicies = Preconditions.checkNotNull(builder.repairPolicies,
                "Repair policies cannot be null");
        myRepairHistory = Preconditions.checkNotNull(builder.repairHistory,
                "Repair history must be set");
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get repair configuration.
     *
     * @return RepairConfiguration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    /**
     * Get scheduled repair job view.
     *
     * @return ScheduledRepairJobView
     */
    public ScheduledRepairJobView getView()
    {
        long now = System.currentTimeMillis();
        return new ScheduledRepairJobView(getId(), myTableReference, myRepairConfiguration, myRepairState.getSnapshot(),
                getStatus(now), getProgress(now), getNextRunInMs());
    }

    private long getNextRunInMs()
    {
        return (getLastSuccessfulRun() + getRepairConfiguration().getRepairIntervalInMs()) - getRunOffset();
    }

    private double getProgress(final long timestamp)
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

    private Predicate<VnodeRepairState> isRepaired(final long timestamp, final long interval)
    {
        return state -> timestamp - state.lastRepairedAt() <= interval;
    }

    private ScheduledRepairJobView.Status getStatus(final long timestamp)
    {
        if (getRealPriority() != -1 && !super.runnable())
        {
            return ScheduledRepairJobView.Status.BLOCKED;
        }
        long repairedAt = myRepairState.getSnapshot().lastCompletedAt();
        long msSinceLastRepair = timestamp - repairedAt;
        RepairConfiguration config = myRepairConfiguration;

        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return ScheduledRepairJobView.Status.OVERDUE;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return ScheduledRepairJobView.Status.LATE;
        }
        if (msSinceLastRepair >= (config.getRepairIntervalInMs() - getRunOffset()))
        {
            return ScheduledRepairJobView.Status.ON_TIME;
        }
        return ScheduledRepairJobView.Status.COMPLETED;
    }

    /**
     * Iterator for scheduled tasks built up by repair groups.
     *
     * @return Scheduled task iterator
     */
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
                        .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                        .withTokensPerRepair(tokensPerRepair)
                        .withRepairPolicies(myRepairPolicies)
                        .withRepairHistory(myRepairHistory)
                        .withJobId(getId());

                taskList.add(builder.build(getRealPriority(replicaRepairGroup.getLastCompletedAt())));
            }

            return taskList.iterator();
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    /**
     * Update the state and set if the task was successful.
     *
     * @param successful
     *            If the job ran successfully.
     * @param task
     *            Last task that has completely successful
     */
    @Override
    public void postExecute(final boolean successful, final ScheduledTask task)
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

    /**
     * Get last successful run.
     *
     * @return long
     */
    @Override
    public long getLastSuccessfulRun()
    {
        return myRepairState.getSnapshot().lastCompletedAt();
    }

    /**
     * Get run offset.
     *
     * @return long
     */
    @Override
    public long getRunOffset()
    {
        return myRepairState.getSnapshot().getEstimatedRepairTime();
    }

    /**
     * Runnable.
     *
     * @return boolean
     */
    @Override
    public boolean runnable()
    {
        return myRepairState.getSnapshot().canRepair() && super.runnable();
    }

    /**
     * Refresh the repair state.
     */
    @Override
    public void refreshState()
    {
        try
        {
            myRepairState.update();
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }
    }

     /**
     * Calculate real priority based on available tasks.
     * @return priority
     */
    @Override
    public final int getRealPriority()
    {
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();
        int priority = -1;
        if (repairStateSnapshot.canRepair())
        {
            long minRepairedAt = System.currentTimeMillis();
            for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                long replicaGroupCompletedAt = replicaRepairGroup.getLastCompletedAt();
                if (replicaGroupCompletedAt < minRepairedAt)
                {
                    minRepairedAt = replicaGroupCompletedAt;
                }
            }
            priority = getRealPriority(minRepairedAt);
        }
        return priority;
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    private BigInteger getTokensPerRepair(final VnodeRepairStates vnodeRepairStates)
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

    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private RepairState repairState;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private TableStorageStates tableStorageStates;
        private final List<TableRepairPolicy> repairPolicies = new ArrayList<>();
        private RepairHistory repairHistory;

        /**
         * Build table repair job with configuration.
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
         * Build table repair job with table reference.
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
         * Build table repair job with JMX proxy factory.
         *
         * @param aJMXProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory aJMXProxyFactory)
        {
            this.jmxProxyFactory = aJMXProxyFactory;
            return this;
        }

        /**
         * Build table repair job with repair state.
         *
         * @param theRepairState Repair state.
         * @return Builder
         */
        public Builder withRepairState(final RepairState theRepairState)
        {
            this.repairState = theRepairState;
            return this;
        }

        /**
         * Build table repair job with table repair metrics.
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
         * Build table repair job with repair configuration.
         *
         * @param theRepairConfiguration The repair confiuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration theRepairConfiguration)
        {
            this.repairConfiguration = theRepairConfiguration;
            return this;
        }

        /**
         * Build table repair job with repair lock type.
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
         * Build table repair job with table storage states.
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
         * Build table repair job with repair policies.
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
         * Build table repair job with repair history.
         *
         * @param aRepairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory aRepairHistory)
        {
            this.repairHistory = aRepairHistory;
            return this;
        }

        /**
         * Build table repair job.
         *
         * @return TableRepairJob
         */
        public TableRepairJob build()
        {
            Preconditions.checkNotNull(tableReference, "Table reference must be set");

            return new TableRepairJob(this);
        }
    }
}
