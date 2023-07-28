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

import com.ericsson.bss.cassandra.ecchronos.core.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class IncrementalRepairJob extends ScheduledRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;
    private final ReplicationState myReplicationState;
    private final CassandraMetrics myCassandraMetrics;

    IncrementalRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myTableReference, builder.myJmxProxyFactory,
                builder.myRepairConfiguration, builder.myRepairLockType, builder.myRepairPolicies,
                builder.myTableRepairMetrics);
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState, "Replication state must be set");
        myCassandraMetrics = Preconditions.checkNotNull(builder.myCassandraMetrics, "Cassandra metrics must be set");
        setLastSuccessfulRun();
    }

    private void setLastSuccessfulRun()
    {
        myLastSuccessfulRun = myCassandraMetrics.getMaxRepairedAt(getTableReference());
        LOG.debug("{} - last successful run: {}", this, myLastSuccessfulRun);
    }

    /**
     * Get scheduled repair job view.
     *
     * @return ScheduledRepairJobView
     */
    @Override
    public ScheduledRepairJobView getView()
    {
        long now = System.currentTimeMillis();
        return new ScheduledRepairJobView(getId(), getTableReference(), getRepairConfiguration(), getStatus(now),
                getProgress(), getNextRunInMs(), getLastSuccessfulRun(), RepairOptions.RepairType.INCREMENTAL);
    }

    private long getNextRunInMs()
    {
        return getLastSuccessfulRun() + getRepairConfiguration().getRepairIntervalInMs();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private double getProgress()
    {
        return myCassandraMetrics.getPercentRepaired(getTableReference()) / 100d;
    }

    private ScheduledRepairJobView.Status getStatus(final long timestamp)
    {
        if (getRealPriority() != -1 && !super.runnable())
        {
            return ScheduledRepairJobView.Status.BLOCKED;
        }
        long msSinceLastRepair = timestamp - myLastSuccessfulRun;
        if (msSinceLastRepair >= getRepairConfiguration().getRepairErrorTimeInMs())
        {
            return ScheduledRepairJobView.Status.OVERDUE;
        }
        if (msSinceLastRepair >= getRepairConfiguration().getRepairWarningTimeInMs())
        {
            return ScheduledRepairJobView.Status.LATE;
        }
        if (msSinceLastRepair >= (getRepairConfiguration().getRepairIntervalInMs() - getRunOffset()))
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
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(
                myReplicationState.getReplicas(getTableReference()),
                ImmutableList.of(), myLastSuccessfulRun);
        RepairGroup.Builder builder = RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withRepairResourceFactory(getRepairLockType().getLockFactory())
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withReplicaRepairGroup(replicaRepairGroup)
                .withRepairPolicies(getRepairPolicies()).withJobId(getId());
        List<ScheduledTask> taskList = new ArrayList<>();
        taskList.add(builder.build(getRealPriority()));
        return taskList.iterator();
    }

    /**
     * Decides if the job is runnable.
     *
     * @return true if enough time has passed and there is something to repair.
     */
    @Override
    public boolean runnable()
    {
        if (super.runnable())
        {
            boolean nothingToRepair = getProgress() >= 1.0;
            if (nothingToRepair)
            {
                myLastSuccessfulRun = System.currentTimeMillis();
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Incremental repair job of %s", getTableReference());
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
        IncrementalRepairJob that = (IncrementalRepairJob) o;
        return Objects.equals(myReplicationState, that.myReplicationState) && Objects.equals(
                myCassandraMetrics, that.myCassandraMetrics);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myReplicationState, myCassandraMetrics);
    }

    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        Configuration myConfiguration = new ConfigurationBuilder().withPriority(Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS).build();
        private TableReference myTableReference;
        private JmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics = null;
        private ReplicationState myReplicationState;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType myRepairLockType;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private CassandraMetrics myCassandraMetrics;

        /**
         * Build with configuration.
         *
         * @param configuration
         *         Configuration.
         * @return Builder
         */
        public Builder withConfiguration(final Configuration configuration)
        {
            myConfiguration = configuration;
            return this;
        }

        /**
         * Build with table reference.
         *
         * @param tableReference
         *         Table reference.
         * @return Builder
         */
        public Builder withTableReference(final TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        /**
         * Build with JMX proxy factory.
         *
         * @param jmxProxyFactory
         *         JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Build with table repair metrics.
         *
         * @param tableRepairMetrics
         *         Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param repairConfiguration
         *         The repair configuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Build with repair lock type.
         *
         * @param repairLockType
         *         Repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Build with replication state.
         *
         * @param replicationState
         *         Replication state.
         * @return Builder
         */
        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        /**
         * Build table repair job with repair policies.
         *
         * @param repairPolicies
         *         The table repair policies.
         * @return Builder
         */
        public Builder withRepairPolices(final Collection<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies.addAll(repairPolicies);
            return this;
        }

        /**
         * Build with cassandra metrics.
         *
         * @param cassandraMetrics The Cassandra metrics.
         * @return Builder
         */
        public Builder withCassandraMetrics(final CassandraMetrics cassandraMetrics)
        {
            myCassandraMetrics = cassandraMetrics;
            return this;
        }

        /**
         * Build table repair job.
         *
         * @return TableRepairJob
         */
        public IncrementalRepairJob build()
        {
            Preconditions.checkNotNull(myTableReference, "Table reference must be set");

            return new IncrementalRepairJob(this);
        }
    }
}
