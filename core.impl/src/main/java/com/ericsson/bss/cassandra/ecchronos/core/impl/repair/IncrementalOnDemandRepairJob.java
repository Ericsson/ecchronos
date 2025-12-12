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
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
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

public final class IncrementalOnDemandRepairJob extends OnDemandRepairJob
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalOnDemandRepairJob.class);
    private final ReplicationState myReplicationState;
    private final List<ScheduledTask> myTasks;
    private final int myTotalTasks;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    public IncrementalOnDemandRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myJmxProxyFactory, builder.myRepairConfiguration,
                builder.myRepairLockType, builder.myOnFinishedHook, builder.myTableRepairMetrics, builder.myOngoingJob,
                builder.myCurrentNode);
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState,
                "Replication state must be set");
        myTimeBasedRunPolicy = builder.myTimeBasedRunPolicy;
        myTasks = initializeTasks();
        myTotalTasks = myTasks.size();
    }

    private List<ScheduledTask> initializeTasks()
    {
        // Check if repair is blocked by time-based run policy
        if (myTimeBasedRunPolicy != null && !myTimeBasedRunPolicy.shouldRun(getTableReference(), getCurrentNode()))
        {
            LOG.debug("Incremental repair tasks creation skipped for {} - blocked by time-based run policy", getTableReference());
            return new ArrayList<>();
        }

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

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return new ArrayList<>(myTasks).iterator();
    }

    @Override
    public OnDemandRepairJobView getView()
    {
        OnDemandRepairJobView.Status status = getStatus();
        // Check if repair is blocked by time-based run policy first
        if (myTimeBasedRunPolicy != null && !myTimeBasedRunPolicy.shouldRun(getTableReference(), getCurrentNode()))
        {
            status = OnDemandRepairJobView.Status.BLOCKED;
        }

        return new OnDemandRepairJobView(
                getJobId(),
                getOngoingJob().getHostId(),
                getOngoingJob().getTableReference(),
                status,
                getProgress(),
                getOngoingJob().getCompletedTime(), getOngoingJob().getRepairType());
    }

    public double getProgress()
    {
        int finishedTasks = myTotalTasks - myTasks.size();
        return myTotalTasks == 0 || OngoingJob.Status.finished.equals(getOngoingJob().getStatus())
                ? 1
                : (double) finishedTasks / myTotalTasks;
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
        UUID id = getJobId();
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
        // Check if repair is blocked by time-based run policy
        if (myTimeBasedRunPolicy != null && !myTimeBasedRunPolicy.shouldRun(getTableReference(), getCurrentNode()))
        {
            LOG.debug("Incremental repair job with id {} is blocked by time-based run policy", getJobId());
            return State.BLOCKED;
        }
        return myTasks.isEmpty() ? State.FINISHED : State.RUNNABLE;
    }

    @Override
    public String toString()
    {
        return String.format("Incremental On Demand Repair job of %s", getTableReference());
    }

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
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        public final Builder withNode(final Node node)
        {
            this.myCurrentNode = node;
            return this;
        }

        public final Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            this.myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public final Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            this.myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public final Builder withRepairLockType(final RepairLockType repairLockType)
        {
            this.myRepairLockType = repairLockType;
            return this;
        }

        public final Builder withOnFinished(final Consumer<UUID> onFinishedHook)
        {
            this.myOnFinishedHook = onFinishedHook;
            return this;
        }

        public final Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            this.myRepairConfiguration = repairConfiguration;
            return this;
        }

        public final Builder withOngoingJob(final OngoingJob ongoingJob)
        {
            this.myOngoingJob = ongoingJob;
            return this;
        }

        public final Builder withReplicationState(final ReplicationState replicationState)
        {
            this.myReplicationState = replicationState;
            return this;
        }

        /**
         * Build with TimeBasedRunPolicy.
         *
         * @param timeBasedRunPolicy TimeBasedRunPolicy.
         * @return Builder
         */
        public Builder withTimeBasedRunPolicy(final TimeBasedRunPolicy timeBasedRunPolicy)
        {
            myTimeBasedRunPolicy = timeBasedRunPolicy;
            return this;
        }

        public final IncrementalOnDemandRepairJob build()
        {
            return new IncrementalOnDemandRepairJob(this);
        }
    }
}
