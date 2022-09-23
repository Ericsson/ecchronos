/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A factory creating {@link OnDemandRepairJob}'s for tables.
 */
public final class OnDemandRepairSchedulerImpl implements OnDemandRepairScheduler, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandRepairSchedulerImpl.class);
    private static final int ONGOING_JOBS_PERIOD_SECONDS = 10;

    private final Map<UUID, OnDemandRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;
    private final CqlSession mySession;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairHistory myRepairHistory;
    private final OnDemandStatus myOnDemandStatus;
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor();

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
        myExecutor.scheduleAtFixedRate(() -> getOngoingJobs(), 0, ONGOING_JOBS_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    private void getOngoingJobs()
    {
        try
        {
            Set<OngoingJob> ongoingJobs = myOnDemandStatus.getOngoingJobs(myReplicationState);
            ongoingJobs.forEach(j -> scheduleOngoingJob(j));
        }
        catch (Exception e)
        {
            LOG.info("Failed to get ongoing ondemand jobs: {}, automatic retry in {}s", e.getMessage(),
                    ONGOING_JOBS_PERIOD_SECONDS);
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
            for (ScheduledJob job : myScheduledJobs.values())
            {
                descheduleTable(job);
            }
            myScheduledJobs.clear();
            myExecutor.shutdown();
        }
    }

    /**
     * Schedule cluster wide job.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @return Repair job view list
     */
    @Override
    public List<OnDemandRepairJobView> scheduleClusterWideJob(final TableReference tableReference)
            throws EcChronosException
    {
        OnDemandRepairJobView currentJob = scheduleJob(tableReference, true);
        return getAllClusterWideRepairJobs().stream()
                .filter(j -> j.getId().equals(currentJob.getId()))
                .collect(Collectors.toList());
    }

    /**
     * Schedule job.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @return RepairJobView
     */
    @Override
    public OnDemandRepairJobView scheduleJob(final TableReference tableReference) throws EcChronosException
    {
        return scheduleJob(tableReference, false);
    }

    private OnDemandRepairJobView scheduleJob(final TableReference tableReference,
                                              final boolean isClusterWide)
            throws EcChronosException
    {
        synchronized (myLock)
        {
            if (tableReference != null)
            {
                Optional<KeyspaceMetadata> ks = Metadata.getKeyspace(mySession, tableReference.getKeyspace());
                if (ks.isPresent() && Metadata.getTable(ks.get(), tableReference.getTable()).isPresent())
                {
                    OnDemandRepairJob job = getRepairJob(tableReference, isClusterWide);
                    myScheduledJobs.put(job.getId(), job);
                    myScheduleManager.schedule(job);
                    return job.getView();
                }
            }
            throw new EcChronosException("Keyspace and/or table does not exist");
        }
    }

    private void scheduleOngoingJob(final OngoingJob ongoingJob)
    {
        synchronized (myLock)
        {
            OnDemandRepairJob job = getOngoingRepairJob(ongoingJob);
            if (myScheduledJobs.putIfAbsent(job.getId(), job) == null)
            {
                LOG.info("Scheduling ongoing job: {}", job.getId());
                myScheduleManager.schedule(job);
            }
        }
    }

    public List<OnDemandRepairJobView> getActiveRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values().stream()
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
        return myOnDemandStatus.getAllClusterWideJobs().stream()
                .map(job -> getOngoingRepairJob(job))
                .map(OnDemandRepairJob::getView)
                .collect(Collectors.toList());
    }

    /**
     * Get all repair jobs.
     *
     * @return Repair job view list
     */
    @Override
    public List<OnDemandRepairJobView> getAllRepairJobs()
    {
        return myOnDemandStatus.getAllJobs(myReplicationState).stream()
                .map(job -> getOngoingRepairJob(job))
                .map(OnDemandRepairJob::getView)
                .collect(Collectors.toList());
    }

    private void removeScheduledJob(final UUID id)
    {
        synchronized (myLock)
        {
            ScheduledJob job = myScheduledJobs.remove(id);
            myScheduleManager.deschedule(job);
        }
    }

    private void descheduleTable(final ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job);
        }
    }

    private OnDemandRepairJob getRepairJob(final TableReference tableReference, final boolean isClusterWide)
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withTableReference(tableReference)
                .withReplicationState(myReplicationState)
                .withHostId(myOnDemandStatus.getHostId())
                .build();
        if (isClusterWide)
        {
            ongoingJob.startClusterWideJob();
        }
        OnDemandRepairJob job = new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(this::removeScheduledJob)
                .withRepairConfiguration(myRepairConfiguration)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(ongoingJob)
                .build();
        return job;
    }

    private OnDemandRepairJob getOngoingRepairJob(final OngoingJob ongoingJob)
    {
        OnDemandRepairJob job = new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(this::removeScheduledJob)
                .withRepairConfiguration(myRepairConfiguration)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(ongoingJob)
                .build();
        return job;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private JmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private ScheduleManager myScheduleManager;
        private ReplicationState myReplicationState;
        private RepairLockType repairLockType;
        private CqlSession session;
        private RepairConfiguration repairConfiguration;
        private RepairHistory repairHistory;
        private OnDemandStatus onDemandStatus;

        /**
         * Build on demand repair scheduler with JMX proxy factory.
         *
         * @param theJMXProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory theJMXProxyFactory)
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
