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
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.IncrementalOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.VnodeOnDemandRepairJob;
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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory creating {@link OnDemandRepairJob}'s for tables.
 */
public final class OnDemandRepairSchedulerImpl implements OnDemandRepairScheduler, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandRepairSchedulerImpl.class);
    private static final String ON_DEMAND_JOB_FAIL = "Failed to get ongoing on demand jobs: {}, automatic retry in {}s";
    private static final int ONGOING_JOBS_PERIOD_SECONDS = 10;

    private final Map<UUID, OnDemandRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;
    private final CqlSession mySession;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairHistory myRepairHistory;
    private final OnDemandStatus myOnDemandStatus;
    private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;

    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("OngoingJobsScheduler-%d").build());

    private OnDemandRepairSchedulerImpl(final Builder builder)
    {
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myScheduleManager = builder.myScheduleManager;
        myReplicationState = builder.myReplicationState;
        myRepairLockType = builder.repairLockType;
        mySession = builder.session;
        myRepairConfiguration = builder.repairConfiguration;
        myRepairHistory = builder.repairHistory;
        myOnDemandStatus = builder.onDemandStatus;
        myExecutor.scheduleAtFixedRate(() -> getOngoingStartedJobsForAllNodes(), 0, ONGOING_JOBS_PERIOD_SECONDS, TimeUnit.SECONDS);
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;

    }

    private void getOngoingStartedJobsForAllNodes()
    {
        try
        {
            Map<UUID, Set<OngoingJob>> allOngoingJobs = myOnDemandStatus.getOngoingStartedJobsForAllNodes(myReplicationState);
            allOngoingJobs.values().forEach(jobs -> jobs.forEach(this::scheduleOngoingJob));
        }
        catch (Exception e)
        {
            logFailureMessage(e);
        }
    }

    private static void logFailureMessage(final Exception e)
    {
        LOG.warn(ON_DEMAND_JOB_FAIL,
                e.getMessage(),
                ONGOING_JOBS_PERIOD_SECONDS);
    }

    /**
     * Retrieves and schedules ongoing on-demand repair jobs for a specific host.
     *
     * @param hostId The {@link UUID} representing the host for which to schedule ongoing jobs.
     */
    public void scheduleOngoingJobs(final UUID hostId)
    {
        try
        {
            Set<OngoingJob> ongoingJobs = myOnDemandStatus.getOngoingJobs(myReplicationState, hostId);
            ongoingJobs.forEach(this::scheduleOngoingJob);
        }
        catch (Exception e)
        {
            logFailureMessage(e);
        }
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        synchronized (myLock)
        {
            for (OnDemandRepairJob job : myScheduledJobs.values())
            {
                descheduleTable(job);
            }
            myScheduledJobs.clear();
            myExecutor.shutdown();
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
        List<OnDemandRepairJobView> jobViews = getAllClusterWideRepairJobs().stream()
                .collect(Collectors.toList());
        return jobViews;
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
            OnDemandRepairJob job = getRepairJob(tableReference, isClusterWide, repairType, nodeId);
            myScheduledJobs.put(job.getJobId(), job);
            myScheduleManager.schedule(nodeId, job);
            return job.getView();
        }
    }
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

    private void scheduleOngoingJob(final OngoingJob ongoingJob)
    {
        synchronized (myLock)
        {
            OnDemandRepairJob job = getOngoingRepairJob(ongoingJob);
            if (myScheduledJobs.putIfAbsent(job.getJobId(), job) == null)
            {
                LOG.info("Scheduling ongoing job: {}", job.getJobId());
                myScheduleManager.schedule(ongoingJob.getHostId(), job);
            }
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
                .map(this::getOngoingRepairJob)
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
                .map(this::getOngoingRepairJob)
                .map(OnDemandRepairJob::getView)
                .collect(Collectors.toList());
    }

    private void removeScheduledJob(final UUID id, final UUID hostId)
    {
        synchronized (myLock)
        {
            ScheduledJob job = myScheduledJobs.remove(id);
            myScheduleManager.deschedule(hostId, job);
        }
    }

    private void descheduleTable(final OnDemandRepairJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job.getOngoingJob().getHostId(), job);
        }
    }

    private OnDemandRepairJob getRepairJob(final TableReference tableReference,
                                           final boolean isClusterWide,
                                           final RepairType repairType,
                                           final UUID hostId)
    {
        OngoingJob ongoingJob = createOngoingJob(tableReference, repairType, hostId);
        if (isClusterWide)
        {
            ongoingJob.startClusterWideJob(repairType);
        }
        return getOngoingRepairJob(ongoingJob);
    }

    private OngoingJob createOngoingJob(final TableReference tableReference,
                                        final RepairType repairType,
                                        final UUID hostId)
    {
        return new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withTableReference(tableReference)
                .withReplicationState(myReplicationState)
                .withHostId(hostId)
                .withRepairType(repairType)
                .build();
    }

    private OnDemandRepairJob getOngoingRepairJob(final OngoingJob ongoingJob)
    {
        OnDemandRepairJob job;
        Node node = getNodeByHostId(ongoingJob.getHostId());

        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder(myRepairConfiguration)
                .withRepairType(ongoingJob.getRepairType())
                .build();
        if (ongoingJob.getRepairType().equals(RepairType.INCREMENTAL))
        {
            job = buildIncrementalOnDemandRepairJob(ongoingJob, repairConfiguration, node);
        }
        else
        {
            job = buildVnodeOnDemandRepairJob(ongoingJob, repairConfiguration, node);
        }
        return job;
    }

    private VnodeOnDemandRepairJob buildVnodeOnDemandRepairJob(final OngoingJob ongoingJob,
                                                               final RepairConfiguration repairConfiguration,
                                                               final Node node)
    {
        return new VnodeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(id -> removeScheduledJob(id, ongoingJob.getHostId()))
                .withRepairConfiguration(repairConfiguration)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(ongoingJob)
                .withNode(node)
                .build();
    }

    private IncrementalOnDemandRepairJob buildIncrementalOnDemandRepairJob(final OngoingJob ongoingJob,
                                                                           final RepairConfiguration repairConfiguration,
                                                                           final Node node)
    {
        return new IncrementalOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(id -> removeScheduledJob(id, ongoingJob.getHostId()))
                .withRepairConfiguration(repairConfiguration)
                .withReplicationState(myReplicationState)
                .withOngoingJob(ongoingJob)
                .withNode(node)
                .build();
    }

    private Node getNodeByHostId(final UUID hostId)
    {
        Node node = myOnDemandStatus.getNodes().get(hostId);
        if (node == null)
        {
            throw new NoSuchElementException("No node found with host ID: " + hostId);
        }
        return node;
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
        public Builder withRepairConfiguration(final RepairConfiguration theRepairConfiguration)
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

        public Builder withRepairConfiguration(final Function<TableReference, Set<RepairConfiguration>>
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
