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
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.driver.core.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;

import com.datastax.driver.core.Metadata;
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
    private final Map<UUID, OnDemandRepairJob> myScheduledJobs = new HashMap<>();
    private final Object myLock = new Object();

    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ScheduleManager myScheduleManager;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;
    private final Metadata myMetadata;

    private OnDemandRepairSchedulerImpl(Builder builder)
    {
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myScheduleManager = builder.myScheduleManager;
        myReplicationState = builder.myReplicationState;
        myRepairLockType = builder.repairLockType;
        myMetadata = builder.metadata;
    }

    @Override
    public void close()
    {
        synchronized (myLock)
        {
            for (UUID uuid : myScheduledJobs.keySet())
            {
                ScheduledJob job = myScheduledJobs.get(uuid);

                descheduleTable(job);
            }

            myScheduledJobs.clear();
        }
    }

    @Override
    public RepairJobView scheduleJob(TableReference tableReference) throws EcChronosException
    {
        synchronized (myLock)
        {
            KeyspaceMetadata ks = myMetadata.getKeyspace(tableReference.getKeyspace());
            if (ks == null)
            {
                throw new EcChronosException("Keyspace does not exist");
            }
            if (ks.getTable(tableReference.getTable()) == null)
            {
                throw new EcChronosException("Table does not exist");
            }
            OnDemandRepairJob job = getRepairJob(tableReference);
            myScheduledJobs.put(job.getId(), job);
            myScheduleManager.schedule(job);
            return job.getView();
        }
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

    private void removeScheduledJob(UUID id)
    {
        synchronized (myLock)
        {
            myScheduledJobs.remove(id);
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
        private Metadata metadata;

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

        public Builder withMetadata(Metadata metadata)
        {
            this.metadata = metadata;
            return this;
        }

        public OnDemandRepairSchedulerImpl build()
        {
            return new OnDemandRepairSchedulerImpl(this);
        }
    }
}
