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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A factory creating {@link TableRepairJob}'s for tables based on the provided repair configuration.
 */
public final class RepairSchedulerImpl implements RepairScheduler, Closeable
{
    private static final int TERMINATION_WAIT = 10;

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

    private RepairSchedulerImpl(final Builder builder)
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
            if (!myExecutor.awaitTermination(TERMINATION_WAIT, TimeUnit.SECONDS))
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
    public void putConfiguration(final TableReference tableReference,
                                 final RepairConfiguration repairConfiguration)
    {
        myExecutor.execute(() -> handleTableConfigurationChange(tableReference, repairConfiguration));
    }

    @Override
    public void removeConfiguration(final TableReference tableReference)
    {
        myExecutor.execute(() -> handleTableConfigurationRemoved(tableReference));
    }

    @Override
    public List<ScheduledRepairJobView> getCurrentRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values().stream()
                    .map(TableRepairJob::getView)
                    .collect(Collectors.toList());
        }
    }

    private void handleTableConfigurationChange(final TableReference tableReference,
                                                final RepairConfiguration repairConfiguration)
    {
        synchronized (myLock)
        {
            try
            {
                if (configurationHasChanged(tableReference, repairConfiguration))
                {
                    createTableSchedule(tableReference, repairConfiguration);
                }
            }
            catch (Exception e)
            {
                LOG.error("Unexpected error during schedule change of {}:", tableReference, e);
            }
        }
    }

    private boolean configurationHasChanged(final TableReference tableReference,
                                            final RepairConfiguration repairConfiguration)
    {
        TableRepairJob tableRepairJob = myScheduledJobs.get(tableReference);

        return tableRepairJob == null || !repairConfiguration.equals(tableRepairJob.getRepairConfiguration());
    }

    private void createTableSchedule(final TableReference tableReference,
                                     final RepairConfiguration repairConfiguration)
    {
        TableRepairJob oldTableRepairJob = myScheduledJobs.get(tableReference);

        descheduleTableJob(oldTableRepairJob);

        TableRepairJob job = getRepairJob(tableReference, repairConfiguration);
        myScheduledJobs.put(tableReference, job);
        myScheduleManager.schedule(job);
    }

    private void handleTableConfigurationRemoved(final TableReference tableReference)
    {
        synchronized (myLock)
        {
            try
            {
                ScheduledJob job = myScheduledJobs.remove(tableReference);
                descheduleTableJob(job);
            }
            catch (Exception e)
            {
                LOG.error("Unexpected error during schedule removal of {}:", tableReference, e);
            }
        }
    }

    private void descheduleTableJob(final ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job);
        }
    }

    private TableRepairJob getRepairJob(final TableReference tableReference,
                                        final RepairConfiguration repairConfiguration)
    {
        long repairIntervalInMs = repairConfiguration.getRepairIntervalInMs();

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairIntervalInMs, TimeUnit.MILLISECONDS)
                .build();
        AlarmPostUpdateHook alarmPostUpdateHook = new AlarmPostUpdateHook(tableReference,
                repairConfiguration, myFaultReporter);
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

        /**
         * RepairSchedulerImpl build with fault reporter.
         */
        public Builder withFaultReporter(final RepairFaultReporter repairFaultReporter)
        {
            myFaultReporter = repairFaultReporter;
            return this;
        }

        /**
         * RepairSchedulerImpl build with JMX proxy factory.
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * RepairSchedulerImpl build with table repair metrics.
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * RepairSchedulerImpl build with schedule manager.
         */
        public Builder withScheduleManager(final ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair state factory.
         */
        public Builder withRepairStateFactory(final RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair lock type.
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * RepairSchedulerImpl build with table storage states.
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair policies.
         */
        public Builder withRepairPolicies(final Collection<TableRepairPolicy> tableRepairPolicies)
        {
            myRepairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        /**
         * RepairSchedulerImpl build with repair history.
         */
        public Builder withRepairHistory(final RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        /**
         * RepairSchedulerImpl build.
         */
        public RepairSchedulerImpl build()
        {
            return new RepairSchedulerImpl(this);
        }
    }
}
