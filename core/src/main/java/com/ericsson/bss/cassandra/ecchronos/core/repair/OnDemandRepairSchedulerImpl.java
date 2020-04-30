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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A factory creating {@link OnDemandRepairJob}'s for tables.
 */
public class OnDemandRepairSchedulerImpl implements OnDemandRepairScheduler, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandRepairSchedulerImpl.class);

    private final Map<TableReference, OnDemandRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final ExecutorService myExecutor;

    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;

    private OnDemandRepairSchedulerImpl(Builder builder)
    {
        myExecutor = Executors.newSingleThreadScheduledExecutor();
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myScheduleManager = builder.myScheduleManager;
        myReplicationState = builder.myReplicationState;
        myRepairLockType = builder.repairLockType;
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

                descheduleTable(job);
            }

            myScheduledJobs.clear();
        }
    }

    @Override
    public RepairJobView scheduleJob(TableReference tableReference)
    {
        OnDemandRepairJob job = getRepairJob(tableReference);
        myExecutor.execute(() -> scheduleRepairJob(tableReference, job));
        return job.getView();
    }

    @Override
    public List<RepairJobView> getCurrentRepairJobs()
    {
        synchronized (myLock)
        {
            return myScheduledJobs.values().stream()
                    .map(OnDemandRepairJob::getView)
                    .collect(Collectors.toList());
        }
    }

    private OnDemandRepairJob scheduleRepairJob(TableReference tableReference, OnDemandRepairJob job)
    {
        myScheduledJobs.put(tableReference, job);
        myScheduleManager.schedule(job);
        return job;
    }

    private void removeScheduledJob(TableReference tableReference)
    {
        synchronized (myLock)
        {
            myScheduledJobs.remove(tableReference);
        }
    }

    private void descheduleTable(ScheduledJob job)
    {
        if (job != null)
        {
            myScheduleManager.deschedule(job);
        }
    }

    private OnDemandRepairJob getRepairJob(TableReference tableReference)
    {
        OnDemandRepairJob job = new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(tableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withReplicationState(myReplicationState)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(this::removeScheduledJob)
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

        public Builder withReplicationState(ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            this.repairLockType = repairLockType;
            return this;
        }

        public OnDemandRepairSchedulerImpl build()
        {
            return new OnDemandRepairSchedulerImpl(this);
        }
    }
}
