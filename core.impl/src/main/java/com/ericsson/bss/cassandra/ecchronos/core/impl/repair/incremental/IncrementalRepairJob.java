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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Collection;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Class used to run Incremental Repairs in Cassandra.
 */
public class IncrementalRepairJob extends ScheduledRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;
    private final Node myNode;
    private final ReplicationState myReplicationState;
    private final CassandraMetrics myCassandraMetrics;

    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    IncrementalRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myNodeId, builder.myTableReference, builder.myJmxProxyFactory,
                builder.myRepairConfiguration, builder.myRepairPolicies, builder.myTableRepairMetrics, builder.myRepairLockType);
        myNode = Preconditions.checkNotNull(builder.myNode, "Node must be set");
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState, "Replication state must be set");
        myCassandraMetrics = Preconditions.checkNotNull(builder.myCassandraMetrics, "Cassandra metrics must be set");
        setLastSuccessfulRun();
    }

    private void setLastSuccessfulRun()
    {
        myLastSuccessfulRun = myCassandraMetrics.getMaxRepairedAt(myNode.getHostId(), getTableReference());
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
        return new ScheduledRepairJobView(getNodeId(), getJobId(), getTableReference(), getRepairConfiguration(),
                getStatus(now),
                getProgress(), getNextRunInMs(), getLastSuccessfulRun(), getRepairConfiguration().getRepairType());
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

    private long getNextRunInMs()
    {
        return getLastSuccessfulRun() + getRepairConfiguration().getRepairIntervalInMs();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private double getProgress()
    {
        return myCassandraMetrics.getPercentRepaired(myNode.getHostId(), getTableReference()) / 100d;
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
                myReplicationState.getReplicas(getTableReference(), myNode),
                ImmutableList.of(), myLastSuccessfulRun);
        RepairGroup.Builder builder = RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withReplicaRepairGroup(replicaRepairGroup)
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withRepairResourceFactory(getRepairLockType().getLockFactory())
                .withRepairPolicies(getRepairPolicies()).withJobId(getJobId());
        List<ScheduledTask> taskList = new ArrayList<>();
        taskList.add(builder.build(getRealPriority()));
        return taskList.iterator();
    }

    /**
     * Check if there's anything to repair, if not then just move the last run.
     */
    @Override
    public void refreshState()
    {
        boolean nothingToRepair = getProgress() >= 1.0;
        if (nothingToRepair)
        {
            myLastSuccessfulRun = System.currentTimeMillis();
        }
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Incremental repair job of %s in node %s", getTableReference(), myNode.getHostId());
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        else if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }
        IncrementalRepairJob that = (IncrementalRepairJob) o;
        return Objects.equals(myReplicationState, that.myReplicationState) && Objects.equals(
                myCassandraMetrics, that.myCassandraMetrics) && Objects.equals(myNode, that.myNode);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myReplicationState, myCassandraMetrics, myNode);
    }

    /**
     * Builder class to construct IncrementalRepairJob.
     */
    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        ScheduledJob.Configuration myConfiguration = new ScheduledJob.ConfigurationBuilder().withPriority(
                        ScheduledJob.Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS).build();
        private TableReference myTableReference;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private Node myNode;
        private UUID myNodeId;
        private TableRepairMetrics myTableRepairMetrics = null;
        private ReplicationState myReplicationState;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private CassandraMetrics myCassandraMetrics;
        private RepairLockType myRepairLockType;

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
         * Build with configuration.
         *
         * @param configuration
         *         Configuration.
         * @return Builder
         */
        public Builder withConfiguration(final ScheduledJob.Configuration configuration)
        {
            myConfiguration = configuration;
            return this;
        }

        /**
         * Build with configuration.
         *
         * @param node
         *         Node.
         * @return Builder
         */
        public Builder withNode(final Node node)
        {
            myNode = node;
            myNodeId =  myNode.getHostId();
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
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
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

