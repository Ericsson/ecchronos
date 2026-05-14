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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * A factory creating {@link OnDemandRepairJob}'s for tables.
 */
public final class OnDemandRepairSchedulerImpl implements OnDemandRepairScheduler, Closeable
{

    private final Map<UUID, OnDemandRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final ScheduleManager myScheduleManager;
    private final CqlSession mySession;
    private final OnDemandStatus myOnDemandStatus;
    private final ReplicationState myReplicationState;
    private final RepairConfiguration myRepairConfiguration;
    private final OnDemandRepairJobFactory myJobFactory;
    private final OnDemandJobPoller myPoller;
    private final Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;

    private OnDemandRepairSchedulerImpl(final Builder builder)
    {
        myScheduleManager = builder.myScheduleManager;
        mySession = builder.session;
        myOnDemandStatus = builder.onDemandStatus;
        myReplicationState = builder.myReplicationState;
        myRepairConfiguration = builder.repairConfiguration;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;

        myJobFactory = OnDemandRepairJobFactory.builder()
                .withJmxProxyFactory(builder.myJmxProxyFactory)
                .withTableRepairMetrics(builder.myTableRepairMetrics)
                .withReplicationState(builder.myReplicationState)
                .withRepairLockType(builder.repairLockType)
                .withRepairHistory(builder.repairHistory)
                .withRepairConfiguration(builder.repairConfiguration)
                .withOnDemandStatus(builder.onDemandStatus)
                .withOnFinishedHook(this::removeScheduledJob)
                .build();

        myPoller = OnDemandJobPoller.builder()
                .withOnDemandStatus(builder.onDemandStatus)
                .withReplicationState(builder.myReplicationState)
                .withScheduleManager(builder.myScheduleManager)
                .withJobFactory(myJobFactory)
                .withTryAddJob(this::tryAddJob)
                .withRemoveFinishedJobs(this::removeFinishedJobs)
                .build();
    }

    private Boolean tryAddJob(final OnDemandRepairJob job)
    {
        synchronized (myLock)
        {
            return myScheduledJobs.putIfAbsent(job.getJobId(), job) == null;
        }
    }

    private void removeFinishedJobs()
    {
        synchronized (myLock)
        {
            myScheduledJobs.entrySet().removeIf(entry ->
            {
                ScheduledJob.State state = entry.getValue().getState();
                return state == ScheduledJob.State.FINISHED || state == ScheduledJob.State.FAILED;
            });
        }
    }

    /**
     * Retrieves and schedules ongoing on-demand repair jobs for a specific host.
     *
     * @param hostId The {@link UUID} representing the host for which to schedule ongoing jobs.
     */
    public void scheduleOngoingJobs(final UUID hostId)
    {
        myPoller.scheduleOngoingJobs(hostId);
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        myPoller.close();
        synchronized (myLock)
        {
            for (OnDemandRepairJob job : myScheduledJobs.values())
            {
                descheduleTable(job);
            }
            myScheduledJobs.clear();
        }
    }

    /**
     * Schedule cluster wide job on any random available up node.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @return Repair job view list
     */
    @Override
    public List<OnDemandRepairJobView> scheduleClusterWideJob(final TableReference tableReference,
                                                              final RepairType repairType) throws EcChronosException
    {
        for (Node node : myOnDemandStatus.getNodes().values())
        {
            scheduleJob(tableReference, true, repairType, node.getHostId());
        }
        return getAllClusterWideRepairJobs().stream().collect(Collectors.toList());
    }

    /**
     * Schedule job on particular node.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @param repairType The repair type for the on demand repair.
     * @return RepairJobView
     */
    @Override
    public OnDemandRepairJobView
           scheduleJob(final TableReference tableReference, final RepairType repairType, final UUID nodeId) throws EcChronosException
    {
        return scheduleJob(tableReference, false, repairType, nodeId);
    }

    private OnDemandRepairJobView scheduleJob(final TableReference tableReference,
                                              final boolean isClusterWide,
                                              final RepairType repairType,
                                              final UUID nodeId) throws EcChronosException
    {
        synchronized (myLock)
        {
            validateTableReference(tableReference);
            OnDemandRepairJob job = myJobFactory.createNewJob(tableReference, isClusterWide, repairType, nodeId);
            myScheduledJobs.put(job.getJobId(), job);
            myScheduleManager.schedule(nodeId, job);
            return job.getView();
        }
    }

    @Override
    public boolean checkTableEnabled(final TableReference tableReference, final boolean forceRepairDisabled)
    {
        boolean enabled = true;
        if (!forceRepairDisabled)
        {
            Set<RepairConfiguration> repairConfigurations = myRepairConfigurationFunction.apply(tableReference);
            for (RepairConfiguration repairConfiguration : repairConfigurations)
            {
                if (RepairConfiguration.DISABLED.equals(repairConfiguration))
                {
                    enabled = false;
                }
            }
        }
        return enabled;
    }

    private void validateTableReference(final TableReference tableReference) throws EcChronosException
    {
        if (tableReference == null)
        {
            throw new EcChronosException("Table reference cannot be null");
        }

        Optional<KeyspaceMetadata> keyspace = Metadata.getKeyspace(mySession, tableReference.getKeyspace());
        if (keyspace.isEmpty() || Metadata.getTable(keyspace.get(), tableReference.getTable()).isEmpty())
        {
            throw new EcChronosException("Keyspace and/or table does not exist");
        }
    }

    public List<OnDemandRepairJobView> getActiveRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values()
                    .stream()
                    .map(OnDemandRepairJob::getView)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Get all cluster wide repair jobs.
     *
     * @return Repair job view list
     */
    @Override
    public List<OnDemandRepairJobView> getAllClusterWideRepairJobs()
    {
        return myOnDemandStatus.getAllClusterWideJobs()
                .stream()
                .map(myJobFactory::createFromOngoingJob)
                .map(OnDemandRepairJob::getView)
                .collect(Collectors.toList());
    }

    /**
     * Get all repair jobs for specific host.
     *
     * @return Repair job view list
     */
    @Override
    public List<OnDemandRepairJobView> getAllRepairJobs(final UUID hostId)
    {
        return myOnDemandStatus.getAllJobs(myReplicationState, hostId)
                .stream()
                .map(myJobFactory::createFromOngoingJob)
                .map(OnDemandRepairJob::getView)
                .collect(Collectors.toList());
    }

    private void removeScheduledJob(final UUID id, final UUID hostId)
    {
        synchronized (myLock)
        {
            ScheduledJob job = myScheduledJobs.remove(id);
            if (job != null)
            {
                myScheduleManager.deschedule(hostId, job);
            }
        }
    }

    private void descheduleTable(final OnDemandRepairJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job.getOngoingJob().getHostId(), job);
        }
    }

    @Override
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private ScheduleManager myScheduleManager;
        private ReplicationState myReplicationState;
        private RepairLockType repairLockType;
        private CqlSession session;
        private RepairConfiguration repairConfiguration;
        private RepairHistory repairHistory;
        private OnDemandStatus onDemandStatus;
        private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;

        /**
         * Build on demand repair scheduler with JMX proxy factory.
         *
         * @param theJMXProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory theJMXProxyFactory)
        {
            myJmxProxyFactory = theJMXProxyFactory;
            return this;
        }

        /**
         * Build on demand repair scheduler with table repair metrics.
         *
         * @param theTableRepairMetrics Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics theTableRepairMetrics)
        {
            myTableRepairMetrics = theTableRepairMetrics;
            return this;
        }

        /**
         * Build on demand repair scheduler with scheule manager.
         *
         * @param theScheduleManager Schedule manager.
         * @return Builder
         */
        public Builder withScheduleManager(final ScheduleManager theScheduleManager)
        {
            myScheduleManager = theScheduleManager;
            return this;
        }

        /**
         * Build on demand repair scheduler with replication state.
         *
         * @param theReplicationState Replication state.
         * @return Builder
         */
        public Builder withReplicationState(final ReplicationState theReplicationState)
        {
            myReplicationState = theReplicationState;
            return this;
        }

        /**
         * Build on demand repair scheduler with repair lock type.
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
         * Build on demand repair scheduler with session.
         *
         * @param theSession Session.
         * @return Builder
         */
        public Builder withSession(final CqlSession theSession)
        {
            this.session = theSession;
            return this;
        }

        /**
         * Build on demand repair scheduler with repair configuration.
         *
         * @param theRepairConfiguration Repair configuration.
         * @return Builder
         */
        public Builder withRepairConfigurationFunction(final RepairConfiguration theRepairConfiguration)
        {
            this.repairConfiguration = theRepairConfiguration;
            return this;
        }

        /**
         * Build on demand repair scheduler with repair history.
         *
         * @param theRepairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory theRepairHistory)
        {
            this.repairHistory = theRepairHistory;
            return this;
        }

        /**
         * Build on demand repair scheduler with repair configuration function.
         *
         * @param defaultRepairConfiguration Repair configuration function.
         * @return Builder
         */
        public Builder withRepairConfigurationFunction(final Function<TableReference, Set<RepairConfiguration>>
                                                                         defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = defaultRepairConfiguration;
            return this;
        }

        /**
         * Build on demand repair scheduler with on demand status.
         *
         * @param theOnDemandStatus Status.
         * @return Builder
         */
        public Builder withOnDemandStatus(final OnDemandStatus theOnDemandStatus)
        {
            this.onDemandStatus = theOnDemandStatus;
            return this;
        }

        /**
         * Build on demand repair scheduler.
         *
         * @return OnDemandRepairSchedulerImpl
         */
        public OnDemandRepairSchedulerImpl build()
        {
            return new OnDemandRepairSchedulerImpl(this);
        }
    }
}
