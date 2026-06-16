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
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;

import java.util.Collections;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.RuntimeMBeanException;

/**
 * Class used to construct repair scheduler.
 */
public final class RepairSchedulerImpl implements RepairScheduler, Closeable
{
    private static final int DEFAULT_TERMINATION_WAIT_IN_SECONDS = 10;
    private static final long SCHEDULE_RETRY_DELAY_SECONDS = 30;

    private static final Logger LOG = LoggerFactory.getLogger(RepairSchedulerImpl.class);

    private final Map<UUID, Map<TableReference, Set<ScheduledRepairJob>>> myScheduledJobs = new ConcurrentHashMap<>();
    private final java.util.concurrent.locks.ReadWriteLock myLock = new java.util.concurrent.locks.ReentrantReadWriteLock();

    private final ScheduledExecutorService myExecutor;
    private final ScheduleManager myScheduleManager;
    private final ScheduledRepairJobFactory myJobFactory;

    private Set<ScheduledRepairJob> validateScheduleMap(final UUID nodeID, final TableReference tableReference)
    {
        if (!myScheduledJobs.containsKey(nodeID))
        {
            Map<TableReference, Set<ScheduledRepairJob>> scheduledJobs = new HashMap<>();
            scheduledJobs.put(tableReference, new HashSet<>());
            myScheduledJobs.put(nodeID, scheduledJobs);
            return myScheduledJobs.get(nodeID).get(tableReference);
        }
        Map<TableReference, Set<ScheduledRepairJob>> nodeJobs = myScheduledJobs.get(nodeID);
        if (!nodeJobs.containsKey(tableReference))
        {
            nodeJobs.put(tableReference, new HashSet<>());
        }
        return nodeJobs.get(tableReference);
    }

    private RepairSchedulerImpl(final Builder builder)
    {
        myExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RepairScheduler-%d").build());
        myScheduleManager = builder.myScheduleManager;
        myJobFactory = ScheduledRepairJobFactory.builder()
                .withTableRepairMetrics(builder.myTableRepairMetrics)
                .withRepairHistoryService(builder.myRepairHistoryService)
                .withFaultReporter(builder.myFaultReporter)
                .withJmxProxyFactory(builder.myJmxProxyFactory)
                .withRepairStateFactory(builder.myRepairStateFactory)
                .withReplicationState(builder.myReplicationState)
                .withCassandraMetrics(builder.myCassandraMetrics)
                .withRepairPolicies(builder.myRepairPolicies)
                .withTableStorageStates(builder.myTableStorageStates)
                .withRepairLockType(builder.myRepairLockType)
                .withTimeBasedRunPolicy(builder.myTimeBasedRunPolicy)
                .build();
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

        myLock.writeLock().lock();
        try
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
        finally
        {
            myLock.writeLock().unlock();
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
        myLock.readLock().lock();
        try
        {
            return myScheduledJobs.values().stream()
                    .flatMap(tableJobs -> tableJobs.values().stream())
                    .flatMap(Set::stream)
                    .map(ScheduledRepairJob::getView)
                    .collect(Collectors.toList());
        }
        finally
        {
            myLock.readLock().unlock();
        }
    }

    @Override
    public List<ScheduledRepairJobView> getCurrentRepairJobsByNode(final UUID nodeId)
    {
        myLock.readLock().lock();
        try
        {
            Map<TableReference, Set<ScheduledRepairJob>> tableJobs = myScheduledJobs.get(nodeId);

            if (tableJobs == null)
            {
                return Collections.emptyList();
            }

            return tableJobs.values().stream()
                    .flatMap(Set::stream)
                    .map(ScheduledRepairJob::getView)
                    .collect(Collectors.toList());
        }
        finally
        {
            myLock.readLock().unlock();
        }
    }


    private void handleTableConfigurationChange(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        myLock.writeLock().lock();
        try
        {
            Set<ScheduledRepairJob> jobs = validateScheduleMap(node.getHostId(), tableReference);
            if (RepairConfigurationComparator.hasChanged(jobs, repairConfigurations))
            {
                LOG.info("Creating schedule for table {} in node {}", tableReference, node.getHostId());
                createTableSchedule(node, tableReference, repairConfigurations);
            }
            else
            {
                LOG.info("No configuration changes for table {} in node {}", tableReference, node.getHostId());
            }
        }
        catch (RuntimeMBeanException e)
        {
            if (e.getCause() instanceof IllegalStateException
                    && e.getCause().getMessage() != null
                    && e.getCause().getMessage().contains("More than one key found"))
            {
                LOG.error("Unexpected error during schedule change of {} on node {}, this is probably due to "
                        + "a connection to a version of the Jolokia Agent 2.3.0 or older", tableReference, node.getHostId());
                LOG.debug("Unexpected error during schedule change of {}:", tableReference, e);
            }
            else
            {
                LOG.error("Unexpected error during schedule change of {}:", tableReference, e);
            }
        }
        catch (Exception e)
        {
            LOG.error("Unexpected error during schedule change of {}, retrying in {}s:",
                    tableReference, SCHEDULE_RETRY_DELAY_SECONDS, e);
            myExecutor.schedule(
                    () -> handleTableConfigurationChange(node, tableReference, repairConfigurations),
                    SCHEDULE_RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
        }
        finally
        {
            myLock.writeLock().unlock();
        }
    }

    private void createTableSchedule(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfigurations)
    {
        Set<ScheduledRepairJob> currentJobs = myScheduledJobs.get(node.getHostId()).get(tableReference);
        Map<TableReference, Set<ScheduledRepairJob>> tableJob = myScheduledJobs.get(node.getHostId());
        if (tableJob == null)
        {
            tableJob = new HashMap<>();
        }
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
            ScheduledRepairJob job = myJobFactory.create(node, tableReference, repairConfiguration);
            newJobs.add(job);
            myScheduleManager.schedule(node.getHostId(), job);
        }
        tableJob.put(tableReference, newJobs);
        myScheduledJobs.put(node.getHostId(), tableJob);
    }

    private void tableConfigurationRemoved(final Node node, final TableReference tableReference)
    {
        myLock.writeLock().lock();
        try
        {
            Map<TableReference, Set<ScheduledRepairJob>> tableJobs = myScheduledJobs.get(node.getHostId());
            if (tableJobs == null)
            {
                LOG.warn("No scheduled jobs found for node {} when removing/updating table config {}", node.getHostId(), tableReference);
                return;
            }
            Set<ScheduledRepairJob> jobs = tableJobs.remove(tableReference);
            if (jobs != null)
            {
                for (ScheduledRepairJob job : jobs)
                {
                    descheduleTableJob(node.getHostId(), job);
                }
            }
        }
        catch (Exception e)
        {
            LOG.error("Unexpected error during schedule removal of {}:", tableReference, e);
        }
        finally
        {
            myLock.writeLock().unlock();
        }
    }

    @Override
    public void removeAllConfigurationsForNode(final UUID nodeId)
    {
        myExecutor.execute(() -> nodeConfigurationRemoved(nodeId));
    }

    private void nodeConfigurationRemoved(final UUID nodeId)
    {
        myLock.writeLock().lock();
        try
        {
            Map<TableReference, Set<ScheduledRepairJob>> tableJobs = myScheduledJobs.remove(nodeId);
            if (tableJobs == null)
            {
                LOG.info("No scheduled jobs found for node {}", nodeId);
                return;
            }
            tableJobs.values().stream()
                    .flatMap(Set::stream)
                    .forEach(job -> descheduleTableJob(nodeId, job));
            LOG.info("All scheduled jobs removed for node {}", nodeId);
        }
        catch (Exception e)
        {
            LOG.error("Unexpected error during schedule removal for node {}:", nodeId, e);
        }
        finally
        {
            myLock.writeLock().unlock();
        }
    }

    private void descheduleTableJob(final UUID nodeID, final ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(nodeID, job);
        }
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
        private RepairLockType myRepairLockType;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        /**
         * RepairSchedulerImpl build with repair lock type.
         *
         * @param repairLockType Repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

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
