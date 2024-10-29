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
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Collection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to construct repair scheduler.
 */
public final class RepairSchedulerImpl implements RepairScheduler, Closeable
{
    private static final int DEFAULT_TERMINATION_WAIT_IN_SECONDS = 10;

    private static final Logger LOG = LoggerFactory.getLogger(RepairSchedulerImpl.class);

    private final Map<UUID, Map<TableReference, Set<ScheduledRepairJob>>> myScheduledJobs = new ConcurrentHashMap<>();
    private final Object myLock = new Object();

    private final ExecutorService myExecutor;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairHistoryService myRepairHistoryService;
    private final RepairFaultReporter myFaultReporter;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final ScheduleManager myScheduleManager;
    private final RepairStateFactory myRepairStateFactory;
    private final ReplicationState myReplicationState;
    private final CassandraMetrics myCassandraMetrics;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableStorageStates myTableStorageStates;

    private Set<ScheduledRepairJob> validateScheduleMap(final UUID nodeID, final TableReference tableReference)
    {
        if (!myScheduledJobs.containsKey(nodeID))
        {
            Map<TableReference, Set<ScheduledRepairJob>> scheduledJobs = new HashMap<>();
            scheduledJobs.put(tableReference, new HashSet<>());
            myScheduledJobs.put(nodeID, scheduledJobs);
            return myScheduledJobs.get(nodeID).get(tableReference);
        }
        return myScheduledJobs.get(nodeID).get(tableReference);
    }

    private RepairSchedulerImpl(final Builder builder)
    {
        myExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RepairScheduler-%d").build());
        myFaultReporter = builder.myFaultReporter;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myScheduleManager = builder.myScheduleManager;
        myRepairStateFactory = builder.myRepairStateFactory;
        myReplicationState = builder.myReplicationState;
        myRepairPolicies = new ArrayList<>(builder.myRepairPolicies);
        myCassandraMetrics = builder.myCassandraMetrics;
        myRepairHistoryService = builder.myRepairHistoryService;
        myTableStorageStates = builder().myTableStorageStates;
    }

    @Override
    public String getCurrentJobStatus()
    {
        return myScheduleManager.getCurrentJobStatus();
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(DEFAULT_TERMINATION_WAIT_IN_SECONDS, TimeUnit.SECONDS))
            {
                LOG.warn("Waited 10 seconds for executor to shutdown, still not shut down");
            }
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupted while waiting for executor to shutdown", e);
            Thread.currentThread().interrupt();
        }

        synchronized (myLock)
        {
            myScheduledJobs.entrySet().stream()
                    .flatMap(nodeEntry -> nodeEntry.getValue().entrySet().stream()
                            .flatMap(tableEntry -> tableEntry.getValue().stream()
                                    .map(job -> new AbstractMap.SimpleEntry<>(nodeEntry.getKey(), job))
                            )
                    )
                    .forEach(entry -> descheduleTableJob(entry.getKey(), entry.getValue()));

            myScheduledJobs.clear();
        }
    }

    @Override
    public void putConfigurations(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfiguration)
    {
        myExecutor.execute(() -> handleTableConfigurationChange(node, tableReference, repairConfiguration));
    }

    @Override
    public void removeConfiguration(final Node node, final TableReference tableReference)
    {
        myExecutor.execute(() -> tableConfigurationRemoved(node, tableReference));
    }

    @Override
    public List<ScheduledRepairJobView> getCurrentRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values().stream()
                    .flatMap(tableJobs -> tableJobs.values().stream())
                    .flatMap(Set::stream)
                    .map(ScheduledRepairJob::getView)
                    .collect(Collectors.toList());
        }
    }

    private void handleTableConfigurationChange(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        synchronized (myLock)
        {
            try
            {
                if (configurationHasChanged(node, tableReference, repairConfigurations))
                {
                    LOG.info("Creating schedule for table {} in node {}", tableReference, node.getHostId());
                    createTableSchedule(node, tableReference, repairConfigurations);
                }
                LOG.info("No configuration changes for table {} in node {}", tableReference, node.getHostId());
            }
            catch (Exception e)
            {
                LOG.error("Unexpected error during schedule change of {}:", tableReference, e);
            }
        }
    }

    private boolean configurationHasChanged(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        Set<ScheduledRepairJob> jobs = validateScheduleMap(node.getHostId(), tableReference);
        if (repairConfigurations == null || repairConfigurations.isEmpty())
        {
            return false;
        }

        if (jobs == null || jobs.isEmpty())
        {
            return true;
        }

        int matching = 0;

        for (ScheduledRepairJob job : jobs)
        {
            for (RepairConfiguration repairConfiguration : repairConfigurations)
            {
                if (job.getRepairConfiguration().equals(repairConfiguration))
                {
                    matching++;
                }
            }
        }
        return matching != repairConfigurations.size();
    }

    private void createTableSchedule(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        Set<ScheduledRepairJob> currentJobs = myScheduledJobs.get(node.getHostId()).get(tableReference);
        Map<TableReference, Set<ScheduledRepairJob>> tableJob = new HashMap<>();
        if (currentJobs != null)
        {
            for (ScheduledRepairJob job : currentJobs)
            {
                descheduleTableJob(node.getHostId(), job);
            }
        }
        Set<ScheduledRepairJob> newJobs = new HashSet<>();
        for (RepairConfiguration repairConfiguration : repairConfigurations)
        {
            ScheduledRepairJob job = createScheduledRepairJob(node, tableReference, repairConfiguration);
            newJobs.add(job);
            myScheduleManager.schedule(node.getHostId(), job);
        }
        tableJob.put(tableReference, newJobs);
        myScheduledJobs.put(node.getHostId(), tableJob);
    }

    private void tableConfigurationRemoved(final Node node, final TableReference tableReference)
    {
        synchronized (myLock)
        {
            try
            {
                Set<ScheduledRepairJob> jobs = myScheduledJobs.get(node.getHostId()).remove(tableReference);
                for (ScheduledRepairJob job : jobs)
                {
                    descheduleTableJob(node.getHostId(), job);
                }
            }
            catch (Exception e)
            {
                LOG.error("Unexpected error during schedule removal of {}:", tableReference, e);
            }
        }
    }

    private void descheduleTableJob(final UUID nodeID, final ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(nodeID, job);
        }
    }

    private ScheduledRepairJob createScheduledRepairJob(
            final Node node,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairConfiguration.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withBackoff(repairConfiguration.getBackoffInMs(), TimeUnit.MILLISECONDS)
                .withPriorityGranularity(repairConfiguration.getPriorityGranularityUnit())
                .build();
        ScheduledRepairJob job;
        if (repairConfiguration.getRepairType().equals(RepairType.INCREMENTAL))
        {
            LOG.info("Creating IncrementalRepairJob for node {}", node.getHostId());
            job = new IncrementalRepairJob.Builder()
                    .withConfiguration(configuration)
                    .withNode(node)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withTableReference(tableReference)
                    .withRepairConfiguration(repairConfiguration)
                    .withTableRepairMetrics(myTableRepairMetrics)
                    .withCassandraMetrics(myCassandraMetrics)
                    .withReplicationState(myReplicationState)
                    .withRepairPolices(myRepairPolicies)
                    .build();
        }
        else
        {
            LOG.info("Creating TableRepairJob for node {}", node.getHostId());
            AlarmPostUpdateHook alarmPostUpdateHook = new AlarmPostUpdateHook(tableReference, repairConfiguration,
                    myFaultReporter);
            RepairState repairState = myRepairStateFactory.create(node, tableReference, repairConfiguration,
                    alarmPostUpdateHook);
            job = new TableRepairJob.Builder()
                    .withConfiguration(configuration)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withTableReference(tableReference)
                    .withRepairState(repairState)
                    .withTableRepairMetrics(myTableRepairMetrics)
                    .withRepairConfiguration(repairConfiguration)
                    .withTableStorageStates(myTableStorageStates)
                    .withRepairPolices(myRepairPolicies)
                    .withRepairHistory(myRepairHistoryService)
                    .withNode(node)
                    .build();
        }
        job.refreshState();
        return job;
    }

    /**
     * Create instance of Builder to construct RepairSchedulerImpl.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder used to construct RepairSchedulerImpl.
     */
    public static class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private RepairFaultReporter myFaultReporter;
        private RepairStateFactory myRepairStateFactory;
        private ScheduleManager myScheduleManager;
        private ReplicationState myReplicationState;
        private CassandraMetrics myCassandraMetrics;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private TableRepairMetrics myTableRepairMetrics;
        private RepairHistoryService myRepairHistoryService;
        private TableStorageStates myTableStorageStates;

        /**
         * RepairSchedulerImpl build with fault reporter.
         *
         * @param repairFaultReporter Repair fault reporter.
         * @return Builder
         */
        public Builder withFaultReporter(final RepairFaultReporter repairFaultReporter)
        {
            myFaultReporter = repairFaultReporter;
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair history.
         *
         * @param repairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistoryService repairHistory)
        {
            myRepairHistoryService = repairHistory;
            return this;
        }

        /**
         * RepairSchedulerImpl build with table storage states.
         *
         * @param tableStorageStates Table storage states.
         * @return Builder
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        /**
         * RepairSchedulerImpl build with JMX proxy factory.
         *
         * @param jmxProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * RepairSchedulerImpl build with schedule manager.
         *
         * @param scheduleManager Schedule manager.
         * @return Builder
         */
        public Builder withScheduleManager(final ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair state factory.
         *
         * @param repairStateFactory Repair state factory.
         * @return Builder
         */
        public Builder withRepairStateFactory(final RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        /**
         * RepairSchedulerImpl build with replication state.
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
         * RepairSchedulerImpl build with repair policies.
         *
         * @param tableRepairPolicies Table repair policies.
         * @return Builder
         */
        public Builder withRepairPolicies(final Collection<TableRepairPolicy> tableRepairPolicies)
        {
            myRepairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        /**
         * Build with cassandra metrics.
         *
         * @param cassandraMetrics Cassandra metrics.
         * @return Builder
         */
        public Builder withCassandraMetrics(final CassandraMetrics cassandraMetrics)
        {
            myCassandraMetrics = cassandraMetrics;
            return this;
        }

        /**
         * RepairSchedulerImpl build with table repair metrics.
         *
         * @param tableRepairMetrics Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * RepairSchedulerImpl build.
         *
         * @return RepairSchedulerImpl
         */
        public RepairSchedulerImpl build()
        {
            return new RepairSchedulerImpl(this);
        }
    }
}

