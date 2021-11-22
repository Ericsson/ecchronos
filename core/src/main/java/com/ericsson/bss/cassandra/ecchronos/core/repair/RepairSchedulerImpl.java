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
import java.util.*;
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

    private final Map<TableReference, TableRepairJob> myScheduledJobs = new HashMap<>();
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
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(10, TimeUnit.SECONDS))
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
            for (TableReference tableReference : myScheduledJobs.keySet())
            {
                ScheduledJob job = myScheduledJobs.get(tableReference);
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

    @Override
    public void removeConfiguration(TableReference tableReference)
    {
        myExecutor.execute(() -> handleTableConfigurationRemoved(tableReference));
    }

    @Override
    public List<RepairJobView> getCurrentRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values().stream()
                    .map(TableRepairJob::getView)
                    .collect(Collectors.toList());
        }
    }

    private void handleTableConfigurationChange(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        synchronized (myLock)
        {
            if (configurationHasChanged(tableReference, repairConfiguration))
            {
                createTableSchedule(tableReference, repairConfiguration);
            }
        }
    }

    private boolean configurationHasChanged(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        TableRepairJob tableRepairJob = myScheduledJobs.get(tableReference);

        return tableRepairJob == null || !repairConfiguration.equals(tableRepairJob.getRepairConfiguration());
    }

    private void createTableSchedule(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        TableRepairJob oldTableRepairJob = myScheduledJobs.get(tableReference);

        descheduleTableJob(oldTableRepairJob);

        TableRepairJob job = getRepairJob(tableReference, repairConfiguration);
        myScheduledJobs.put(tableReference, job);
        myScheduleManager.schedule(job);
    }

    private void handleTableConfigurationRemoved(TableReference tableReference)
    {
        synchronized (myLock)
        {
            ScheduledJob job = myScheduledJobs.remove(tableReference);
            descheduleTableJob(job);
        }
    }

    private void descheduleTableJob(ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job);
        }
    }

    private TableRepairJob getRepairJob(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        long repairIntervalInMs = repairConfiguration.getRepairIntervalInMs();

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairIntervalInMs, TimeUnit.MILLISECONDS)
                .build();
        AlarmPostUpdateHook alarmPostUpdateHook = new AlarmPostUpdateHook(tableReference, repairConfiguration, myFaultReporter);
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
                .build();

        job.runnable();

        return job;
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

        public RepairSchedulerImpl build()
        {
            return new RepairSchedulerImpl(this);
        }
    }
}
