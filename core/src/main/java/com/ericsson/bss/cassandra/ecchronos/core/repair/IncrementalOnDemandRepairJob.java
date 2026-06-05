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

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** On-demand repair job that uses incremental repair strategy. */
public final class IncrementalOnDemandRepairJob extends OnDemandRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalOnDemandRepairJob.class);
    private final ReplicationState myReplicationState;
    private final List<ScheduledTask> myTasks;
    private final int myTotalTasks;
    /**
     * Constructs a new IncrementalOnDemandRepairJob.
     * @param builder the builder to configure
     */
    public IncrementalOnDemandRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myJmxProxyFactory, builder.myRepairConfiguration,
                builder.myRepairLockType, builder.myOnFinishedHook, builder.myTableRepairMetrics, builder.myOngoingJob);
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState,
                "Replication state must be set");
        myTasks = createTasks();
        myTotalTasks = myTasks.size();
    }

    private List<ScheduledTask> createTasks()
    {
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(
                myReplicationState.getReplicas(getTableReference()),
                ImmutableList.of(), -1L);
        RepairGroup.Builder builder = RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withRepairResourceFactory(getRepairLockType().getLockFactory())
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withJobId(getId());
        List<ScheduledTask> taskList = new ArrayList<>();
        taskList.add(builder.build(Priority.HIGHEST.getValue()));
        return taskList;
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return new ArrayList<>(myTasks).iterator();
    }

    @Override
    public OnDemandRepairJobView getView()
    {
        return new OnDemandRepairJobView(
                getId(),
                getOngoingJob().getHostId(),
                getOngoingJob().getTableReference(),
                getStatus(),
                getProgress(),
                getOngoingJob().getCompletedTime(), getOngoingJob().getRepairType());
    }

    /**
     * Returns the progress.
     * @return the progress
     */
    public double getProgress()
    {
        int finishedTasks = myTotalTasks - myTasks.size();
        return myTotalTasks == 0 || OngoingJob.Status.finished.equals(getOngoingJob().getStatus())
                ? 1 : (double) finishedTasks / myTotalTasks;
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
            myTasks.remove(task);
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
            LOG.info("Completed incremental on demand repair: {}", id);
        }
        if (hasFailed())
        {
            getOngoingJob().failJob();
            LOG.error("Failed incremental on demand repair: {}", id);
        }
        super.finishJob();
    }

    @Override
    public State getState()
    {
        if (hasFailed())
        {
            return State.FAILED;
        }
        return myTasks.isEmpty() ? State.FINISHED : State.RUNNABLE;
    }

    @Override
    public String toString()
    {
        return String.format("Incremental On Demand Repair job of %s", getTableReference());
    }

    /** Builder for constructing instances of the enclosing class. */
    public static class Builder
    {
        private final Configuration myConfiguration = new ConfigurationBuilder()
                .withPriority(Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private JmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics = null;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.newBuilder().withRepairType(
                RepairOptions.RepairType.INCREMENTAL).build();
        private RepairLockType myRepairLockType;
        private Consumer<UUID> myOnFinishedHook = table ->
        {
        };
        private OngoingJob myOngoingJob;
        private ReplicationState myReplicationState;

        /** Constructs a new Builder. */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Sets the JMX proxy factory.
         * @param jmxProxyFactory the JMX proxy factory
         * @return this builder
         */
        public final Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            this.myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Sets the table repair metrics.
         * @param tableRepairMetrics the table repair metrics
         * @return this builder
         */
        public final Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            this.myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Sets the repair lock type.
         * @param repairLockType the repair lock type
         * @return this builder
         */
        public final Builder withRepairLockType(final RepairLockType repairLockType)
        {
            this.myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Sets the on finished.
         * @param onFinishedHook the on finished hook
         * @return this builder
         */
        public final Builder withOnFinished(final Consumer<UUID> onFinishedHook)
        {
            this.myOnFinishedHook = onFinishedHook;
            return this;
        }

        /**
         * Sets the repair configuration.
         * @param repairConfiguration the repair configuration
         * @return this builder
         */
        public final Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            this.myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Sets the ongoing job.
         * @param ongoingJob the ongoing job
         * @return this builder
         */
        public final Builder withOngoingJob(final OngoingJob ongoingJob)
        {
            this.myOngoingJob = ongoingJob;
            return this;
        }

        /**
         * Sets the replication state.
         * @param replicationState the replication state
         * @return this builder
         */
        public final Builder withReplicationState(final ReplicationState replicationState)
        {
            this.myReplicationState = replicationState;
            return this;
        }

        /**
         * Builds and returns the instance.
         * @return the built instance
         */
        public final IncrementalOnDemandRepairJob build()
        {
            return new IncrementalOnDemandRepairJob(this);
        }
    }
}
