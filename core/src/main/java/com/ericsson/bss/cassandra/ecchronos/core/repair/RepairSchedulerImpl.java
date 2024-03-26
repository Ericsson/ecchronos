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

import com.ericsson.bss.cassandra.ecchronos.core.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A factory creating {@link TableRepairJob}'s for tables based on the provided repair configuration.
 */
public final class RepairSchedulerImpl implements RepairScheduler, Closeable
{
    private static final int TERMINATION_WAIT = 10;

    private static final Logger LOG = LoggerFactory.getLogger(RepairSchedulerImpl.class);

    private final Map<TableReference, Set<ScheduledRepairJob>> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final ExecutorService myExecutor;

    private final RepairFaultReporter myFaultReporter;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final RepairStateFactory myRepairStateFactory;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;
    private final TableStorageStates myTableStorageStates;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final RepairHistory myRepairHistory;
    private final CassandraMetrics myCassandraMetrics;

    private RepairSchedulerImpl(final Builder builder)
    {
        myExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RepairScheduler-%d").build());
        myFaultReporter = builder.myFaultReporter;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myScheduleManager = builder.myScheduleManager;
        myRepairStateFactory = builder.myRepairStateFactory;
        myReplicationState = builder.myReplicationState;
        myRepairLockType = builder.myRepairLockType;
        myTableStorageStates = builder.myTableStorageStates;
        myRepairPolicies = new ArrayList<>(builder.myRepairPolicies);
        myCassandraMetrics = builder.myCassandraMetrics;
        myRepairHistory = builder.myRepairHistory;
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
                Set<ScheduledRepairJob> jobs = myScheduledJobs.get(tableReference);
                for (ScheduledRepairJob job : jobs)
                {
                    descheduleTableJob(job);
                }
            }

            myScheduledJobs.clear();
        }
    }

    @Override
    public void putConfigurations(final TableReference tableReference,
                                  final Set<RepairConfiguration> repairConfiguration)
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
            List<ScheduledRepairJobView> views = new ArrayList<>();
            for (Set<ScheduledRepairJob> jobs : myScheduledJobs.values())
            {
                for (ScheduledRepairJob job: jobs)
                {
                    views.add(job.getView());
                }
            }
            return views;
        }
    }

    private void handleTableConfigurationChange(final TableReference tableReference,
                                                final Set<RepairConfiguration> repairConfigurations)
    {
        synchronized (myLock)
        {
            try
            {
                if (configurationHasChanged(tableReference, repairConfigurations))
                {
                    createTableSchedule(tableReference, repairConfigurations);
                }
            }
            catch (Exception e)
            {
                LOG.error("Unexpected error during schedule change of {}:", tableReference, e);
            }
        }
    }

    private boolean configurationHasChanged(final TableReference tableReference,
                                            final Set<RepairConfiguration> repairConfigurations)
    {
        Set<ScheduledRepairJob> jobs = myScheduledJobs.get(tableReference);
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

    private void createTableSchedule(final TableReference tableReference,
                                     final Set<RepairConfiguration> repairConfigurations)
    {
        Set<ScheduledRepairJob> jobs = myScheduledJobs.get(tableReference);
        if (jobs != null)
        {
            for (ScheduledRepairJob job : jobs)
            {
                descheduleTableJob(job);
            }
        }
        Set<ScheduledRepairJob> newJobs = new HashSet<>();
        for (RepairConfiguration repairConfiguration : repairConfigurations)
        {
            ScheduledRepairJob job = createScheduledRepairJob(tableReference, repairConfiguration);
            newJobs.add(job);
            myScheduleManager.schedule(job);
        }
        myScheduledJobs.put(tableReference, newJobs);
    }

    private void handleTableConfigurationRemoved(final TableReference tableReference)
    {
        synchronized (myLock)
        {
            try
            {
                Set<ScheduledRepairJob> jobs = myScheduledJobs.remove(tableReference);
                for (ScheduledRepairJob job : jobs)
                {
                    descheduleTableJob(job);
                }
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

    private ScheduledRepairJob createScheduledRepairJob(final TableReference tableReference,
            final RepairConfiguration repairConfiguration)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairConfiguration.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withBackoff(repairConfiguration.getBackoffInMs(), TimeUnit.MILLISECONDS)
                .withPriorityGranularity(repairConfiguration.getPriorityGranularityUnit())
                .build();
        ScheduledRepairJob job;
        if (repairConfiguration.getRepairType().equals(RepairOptions.RepairType.INCREMENTAL))
        {
            job = new IncrementalRepairJob.Builder()
                    .withConfiguration(configuration)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withTableReference(tableReference)
                    .withTableRepairMetrics(myTableRepairMetrics)
                    .withRepairConfiguration(repairConfiguration)
                    .withRepairLockType(myRepairLockType)
                    .withReplicationState(myReplicationState)
                    .withRepairPolices(myRepairPolicies)
                    .withCassandraMetrics(myCassandraMetrics)
                    .build();
        }
        else
        {
            AlarmPostUpdateHook alarmPostUpdateHook = new AlarmPostUpdateHook(tableReference, repairConfiguration,
                    myFaultReporter);
            RepairState repairState = myRepairStateFactory.create(tableReference, repairConfiguration,
                    alarmPostUpdateHook);
            job = new TableRepairJob.Builder()
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

        }
        job.refreshState();
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
        private ReplicationState myReplicationState;
        private RepairLockType myRepairLockType;
        private TableStorageStates myTableStorageStates;
        private RepairHistory myRepairHistory;
        private CassandraMetrics myCassandraMetrics;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();

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
         * RepairSchedulerImpl build with JMX proxy factory.
         *
         * @param jmxProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
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
         * RepairSchedulerImpl build with repair history.
         *
         * @param repairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
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
