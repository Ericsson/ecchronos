/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A factory creating {@link TableRepairJob}'s for tables based on the provided repair configuration.
 */
public class RepairSchedulerImpl implements RepairScheduler, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairSchedulerImpl.class);

    private final Map<UUID, TableRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final ExecutorService myExecutor;

    private final RepairFaultReporter myFaultReporter;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final RepairStateFactory myRepairStateFactory;
    private final RepairLockType myRepairLockType;
    private final TableStorageStates myTableStorageStates;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final RepairHistory myRepairHistory;
    private final RepairStatus myRepairStatus;
    private final Metadata myMetadata;

    private RepairSchedulerImpl(Builder builder)
    {
        myExecutor = Executors.newSingleThreadScheduledExecutor();
        myFaultReporter = builder.myFaultReporter;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myScheduleManager = builder.myScheduleManager;
        myRepairStateFactory = builder.myRepairStateFactory;
        myRepairLockType = builder.myRepairLockType;
        myTableStorageStates = builder.myTableStorageStates;
        myRepairPolicies = new ArrayList<>(builder.myRepairPolicies);
        myRepairHistory = Preconditions.checkNotNull(builder.myRepairHistory, "Repair history must be set");
        myRepairStatus = Preconditions.checkNotNull(builder.myRepairStatus, "Repair status must be set");
        myMetadata = Preconditions.checkNotNull(builder.myMetaData, "Metadata must be set");
        new Thread(this::getOngoingRepairs).start();
    }

    private void getOngoingRepairs()
    {
        boolean done = false;
        Set<OngoingRepair> ongoingRepairs;
        while(!done)
        {
            try
            {
                ongoingRepairs = myRepairStatus.getUnfinishedOnDemandRepairs();
                ongoingRepairs.forEach(j -> scheduleUnfinishedOnDemandRepairs(j));
                done = true;
            }
            catch(Exception e)
            {
                try
                {
                    LOG.info("Failed to get ongoing ondemand repairs during startup: {}, automatic retry in 10s",
                            e.getMessage());
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                }
                catch(InterruptedException e1)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void scheduleUnfinishedOnDemandRepairs(OngoingRepair ongoingRepair)
    {
        synchronized(myLock)
        {
            LOG.info("Repair {} did not finish correctly, will be rescheduled", ongoingRepair.getRepairId());
            TableRepairJob job = getOnDemandRepairJob(ongoingRepair.getTableReference(), RepairConfiguration.DISABLED,
                    ongoingRepair.getRepairId(), ongoingRepair.getStartedAt());
            if(myScheduledJobs.putIfAbsent(job.getId(), job) == null)
            {
                myScheduleManager.schedule(job);
            }
        }
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if(!myExecutor.awaitTermination(10, TimeUnit.SECONDS))
            {
                LOG.warn("Waited 10 seconds for executor to shutdown, still not shut down");
            }
        }
        catch(InterruptedException e)
        {
            LOG.error("Interrupted while waiting for executor to shutdown", e);
            Thread.currentThread().interrupt();
        }

        synchronized(myLock)
        {
            for(UUID id : myScheduledJobs.keySet())
            {
                ScheduledJob job = myScheduledJobs.get(id);
                descheduleTableJob(job);
            }

            myScheduledJobs.clear();
        }
    }

    @Override
    public void putConfiguration(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        myExecutor.execute(() -> handleTableConfigurationChange(tableReference, repairConfiguration));
    }

    private void handleTableConfigurationChange(TableReference tableReference,
            RepairConfiguration repairConfiguration)
    {
        synchronized(myLock)
        {
            try
            {
                if(configurationHasChanged(tableReference, repairConfiguration))
                {
                    Optional<OngoingRepair> unfinishedJob = myRepairStatus.getUnfinishedRepairs().stream()
                            .filter(o -> o.getTriggeredBy().equals(tableReference.getId().toString()))
                            .findFirst();
                    if(unfinishedJob.isPresent())
                    {
                        UUID id = unfinishedJob.get().getRepairId();
                        long startedAt = unfinishedJob.get().getStartedAt();
                        LOG.info("Unfinished scheduled job with id {} and startedAt {}, will reuse for next schedule",
                                id, startedAt);
                        createTableSchedule(tableReference, repairConfiguration, id, startedAt);
                    }
                    else
                    {
                        createTableSchedule(tableReference, repairConfiguration);
                    }
                }
            }
            catch(Exception e)
            {
                LOG.error("Unexpected error during schedule change of {}:", tableReference, e);
            }
        }
    }

    private boolean configurationHasChanged(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        for(TableRepairJob tableRepairJob : myScheduledJobs.values())
        {
            if(!tableRepairJob.isOnDemand() && tableRepairJob.getTableReference().equals(tableReference))
            {
                return !repairConfiguration.equals(tableRepairJob.getRepairConfiguration());
            }
        }
        return true;
    }

    @Override
    public void removeConfiguration(TableReference tableReference)
    {
        myExecutor.execute(() -> handleTableConfigurationRemoved(tableReference));
    }

    @Override
    public List<RepairJobView> getCurrentRepairJobs()
    {
        synchronized(myLock)
        {
            return myScheduledJobs.values().stream()
                    .map(TableRepairJob::getOldView)
                    .collect(Collectors.toList());
        }
    }

    private void handleTableConfigurationRemoved(TableReference tableReference)
    {
        synchronized(myLock)
        {
            try
            {
                for(TableRepairJob tableRepairJob : myScheduledJobs.values())
                {
                    if(!tableRepairJob.isOnDemand() && tableRepairJob.getTableReference().equals(tableReference))
                    {
                        myScheduledJobs.remove(tableRepairJob.getId());
                        descheduleTableJob(tableRepairJob);
                    }
                }
            }
            catch(Exception e)
            {
                LOG.error("Unexpected error during schedule removal of {}:", tableReference, e);
            }
        }
    }

    private void descheduleTableJob(ScheduledJob job)
    {
        if(job != null)
        {
            myScheduleManager.deschedule(job);
        }
    }

    @Override
    public OngoingRepair scheduleOnDemandRepair(TableReference tableReference)
    {
        synchronized(myLock)
        {
            KeyspaceMetadata ks = myMetadata.getKeyspace(tableReference.getKeyspace());
            if(ks != null && ks.getTable(tableReference.getTable()) != null)
            {
                UUID repairId = UUID.randomUUID();
                TableRepairJob job =
                        getOnDemandRepairJob(tableReference, RepairConfiguration.DISABLED, repairId,
                                System.currentTimeMillis());
                myScheduledJobs.put(job.getId(), job);
                myScheduleManager.schedule(job);
                return new OngoingRepair.Builder().withOngoingRepairInfo(repairId, OngoingRepair.Status.started, -1L,
                        -1L, "user", -1).withTableReference(tableReference).build();
            }
        }
        return null;
    }

    private TableRepairJob getOnDemandRepairJob(TableReference tableReference, RepairConfiguration repairConfiguration,
            UUID repairId, long startedAt)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.MILLISECONDS)
                .build();
        RepairState repairState = myRepairStateFactory.create(tableReference, repairConfiguration, null);
        TableRepairJob job = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(tableReference)
                .withRepairState(repairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .withRepairLockType(myRepairLockType)
                .withTableStorageStates(myTableStorageStates)
                .withRepairPolices(myRepairPolicies)
                .withRepairHistory(myRepairHistory)
                .withOnFinished(this::removeJob)
                .withRepairStatus(myRepairStatus)
                .withRepairId(repairId)
                .withStartedAt(startedAt)
                .withJobId(UUID.randomUUID())
                .withTriggeredBy("user")
                .build();
        job.runnable();
        return job;
    }

    @Override
    public List<ScheduleView> getSchedules()
    {
        synchronized(myLock)
        {
            List<ScheduleView> schedules = myScheduledJobs.values().stream()
                    .filter(s -> !s.isOnDemand())
                    .map(TableRepairJob::getView)
                    .collect(Collectors.toList());
            return schedules;
        }
    }

    @Override
    public List<OngoingRepair> getRepairs()
    {
        List<OngoingRepair> jobs = myRepairStatus.getAllRepairs().stream().collect(Collectors.toList());
        return jobs;
    }

    private void reschedule(TableRepairJob oldJob)
    {
        synchronized(myLock)
        {
            LOG.info("job: {} finished, rescheduling", oldJob.getId());
            createTableSchedule(oldJob.getTableReference(), oldJob.getRepairConfiguration(), UUID.randomUUID(),
                    -1L);
        }
    }

    private void createTableSchedule(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        createTableSchedule(tableReference, repairConfiguration, UUID.randomUUID(), -1L);
    }

    private void createTableSchedule(TableReference tableReference, RepairConfiguration repairConfiguration,
            UUID repairId, long startedAt)
    {
        TableRepairJob oldTableRepairJob = myScheduledJobs.get(tableReference.getId());

        descheduleTableJob(oldTableRepairJob);

        TableRepairJob job = getRepairJob(tableReference, repairConfiguration, repairId, startedAt);
        myScheduledJobs.put(job.getId(), job);
        myScheduleManager.schedule(job);
    }

    private TableRepairJob getRepairJob(TableReference tableReference, RepairConfiguration repairConfiguration,
            UUID repairId, long startedAt)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairConfiguration.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .build();
        AlarmPostUpdateHook alarmPostUpdateHook =
                new AlarmPostUpdateHook(tableReference, repairConfiguration, myFaultReporter);
        RepairState repairState = myRepairStateFactory.create(tableReference, repairConfiguration, alarmPostUpdateHook);
        TableRepairJob job = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(tableReference)
                .withRepairState(repairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .withRepairLockType(myRepairLockType)
                .withTableStorageStates(myTableStorageStates)
                .withRepairPolices(myRepairPolicies)
                .withRepairHistory(myRepairHistory)
                .withOnFinished(this::reschedule)
                .withRepairStatus(myRepairStatus)
                .withRepairId(repairId)
                .withJobId(tableReference.getId())
                .withStartedAt(startedAt)
                .withTriggeredBy(tableReference.getId().toString())
                .build();
        job.runnable();
        return job;
    }

    private void removeJob(TableRepairJob oldJob)
    {
        synchronized(myLock)
        {
            myScheduledJobs.remove(oldJob.getId());
            myScheduleManager.deschedule(oldJob);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private RepairFaultReporter myFaultReporter;
        private JmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private ScheduleManager myScheduleManager;
        private RepairStateFactory myRepairStateFactory;
        private RepairLockType myRepairLockType;
        private TableStorageStates myTableStorageStates;
        private RepairHistory myRepairHistory;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private RepairStatus myRepairStatus;
        private Metadata myMetaData;

        public Builder withFaultReporter(RepairFaultReporter repairFaultReporter)
        {
            myFaultReporter = repairFaultReporter;
            return this;
        }

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withScheduleManager(ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        public Builder withRepairStateFactory(RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        public Builder withTableStorageStates(TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        public Builder withRepairPolicies(Collection<TableRepairPolicy> tableRepairPolicies)
        {
            myRepairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        public Builder withRepairHistory(RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        public Builder withRepairStatus(RepairStatus repairStatus)
        {
            myRepairStatus = repairStatus;
            return this;
        }

        public RepairSchedulerImpl build()
        {
            return new RepairSchedulerImpl(this);
        }

        public Builder withMetadata(Metadata metadata)
        {
            myMetaData = metadata;
            return this;
        }
    }
}
