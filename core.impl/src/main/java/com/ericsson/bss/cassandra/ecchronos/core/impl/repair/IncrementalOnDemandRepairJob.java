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

package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
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

/**
 * An on-demand repair job that performs incremental repairs on a table.
 */
public final class IncrementalOnDemandRepairJob extends OnDemandRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalOnDemandRepairJob.class);
    private final ReplicationState myReplicationState;
    private final List<ScheduledTask> myTasks;
    private final int myTotalTasks;

    /**
     * Constructs an incremental on-demand repair job from the provided builder.
     *
     * @param builder the builder containing all configuration.
     */
    public IncrementalOnDemandRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myJmxProxyFactory, builder.myRepairConfiguration,
                builder.myRepairLockType, builder.myOnFinishedHook, builder.myTableRepairMetrics, builder.myOngoingJob,
                builder.myCurrentNode);
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState,
                "Replication state must be set");
        myTasks = initializeTasks();
        myTotalTasks = myTasks.size();
    }

    private List<ScheduledTask> initializeTasks()
    {
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(
                myReplicationState.getReplicas(getTableReference(), getCurrentNode()),
                ImmutableList.of(), -1L);

        RepairGroup.Builder groupBuilder = createRepairGroupBuilder(replicaRepairGroup);
        List<ScheduledTask> taskList = new ArrayList<>();
        taskList.add(groupBuilder.build(Priority.HIGHEST.getValue()));
        return taskList;
    }

    private RepairGroup.Builder createRepairGroupBuilder(final ReplicaRepairGroup replicaRepairGroup)
    {
        return RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withRepairResourceFactory(getRepairLockType().getLockFactory())
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withJobId(getJobId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return new ArrayList<>(myTasks).iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OnDemandRepairJobView getView()
    {
        return new OnDemandRepairJobView(
                getJobId(),
                getOngoingJob().getHostId(),
                getOngoingJob().getTableReference(),
                getStatus(),
                getProgress(),
                getOngoingJob().getCompletedTime(), getOngoingJob().getRepairType());
    }

    /**
     * Gets the current progress of the incremental repair as a ratio between 0.0 and 1.0.
     *
     * @return the progress ratio.
     */
    public double getProgress()
    {
        int finishedTasks = myTotalTasks - myTasks.size();
        return myTotalTasks == 0 || OngoingJob.Status.finished.equals(getOngoingJob().getStatus())
                ? 1
                : (double) finishedTasks / myTotalTasks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void postExecute(final boolean successful, final ScheduledTask task)
    {
        myTasks.remove(task);
        if (!successful)
        {
            LOG.error("Error running {}", task);
            setFailed(true);
        }
        super.postExecute(successful, task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finishJob()
    {
        UUID id = getJobId();
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
        getOnFinishedHook().accept(id);
        super.finishJob();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public State getState()
    {
        if (hasFailed())
        {
            return State.FAILED;
        }
        return myTasks.isEmpty() ? State.FINISHED : State.RUNNABLE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("Incremental On Demand Repair job of %s", getTableReference());
    }

    /**
     * Builder for constructing {@link IncrementalOnDemandRepairJob} instances.
     */
    public static class Builder
    {
        private final Configuration myConfiguration = new ConfigurationBuilder()
                .withPriority(Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics = null;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.newBuilder().withRepairType(RepairType.INCREMENTAL).build();
        private RepairLockType myRepairLockType;
        private Consumer<UUID> myOnFinishedHook = table ->
        {
        };
        private Node myCurrentNode;
        private OngoingJob myOngoingJob;
        private ReplicationState myReplicationState;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Sets the current Cassandra node for the repair job.
         *
         * @param node the current node.
         * @return this builder.
         */
        public final Builder withNode(final Node node)
        {
            this.myCurrentNode = node;
            return this;
        }

        /**
         * Sets the JMX proxy factory.
         *
         * @param jmxProxyFactory the JMX proxy factory.
         * @return this builder.
         */
        public final Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            this.myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Sets the table repair metrics.
         *
         * @param tableRepairMetrics the table repair metrics.
         * @return this builder.
         */
        public final Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            this.myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Sets the repair lock type.
         *
         * @param repairLockType the repair lock type.
         * @return this builder.
         */
        public final Builder withRepairLockType(final RepairLockType repairLockType)
        {
            this.myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Sets the callback to invoke when the job finishes.
         *
         * @param onFinishedHook the on-finished callback.
         * @return this builder.
         */
        public final Builder withOnFinished(final Consumer<UUID> onFinishedHook)
        {
            this.myOnFinishedHook = onFinishedHook;
            return this;
        }

        /**
         * Sets the repair configuration.
         *
         * @param repairConfiguration the repair configuration.
         * @return this builder.
         */
        public final Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            this.myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Sets the ongoing job reference.
         *
         * @param ongoingJob the ongoing job.
         * @return this builder.
         */
        public final Builder withOngoingJob(final OngoingJob ongoingJob)
        {
            this.myOngoingJob = ongoingJob;
            return this;
        }

        /**
         * Sets the replication state.
         *
         * @param replicationState the replication state.
         * @return this builder.
         */
        public final Builder withReplicationState(final ReplicationState replicationState)
        {
            this.myReplicationState = replicationState;
            return this;
        }

        /**
         * Builds the {@link IncrementalOnDemandRepairJob} instance.
         *
         * @return the constructed job.
         */
        public final IncrementalOnDemandRepairJob build()
        {
            return new IncrementalOnDemandRepairJob(this);
        }
    }
}
