/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Factory responsible for creating {@link ScheduledRepairJob} instances.
 */
public final class ScheduledRepairJobFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledRepairJobFactory.class);

    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairHistoryService myRepairHistoryService;
    private final RepairFaultReporter myFaultReporter;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final RepairStateFactory myRepairStateFactory;
    private final ReplicationState myReplicationState;
    private final CassandraMetrics myCassandraMetrics;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableStorageStates myTableStorageStates;
    private final RepairLockType myRepairLockType;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    private ScheduledRepairJobFactory(final Builder builder)
    {
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myRepairHistoryService = builder.myRepairHistoryService;
        myFaultReporter = builder.myFaultReporter;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myRepairStateFactory = builder.myRepairStateFactory;
        myReplicationState = builder.myReplicationState;
        myCassandraMetrics = builder.myCassandraMetrics;
        myRepairPolicies = new ArrayList<>(builder.myRepairPolicies);
        myTableStorageStates = builder.myTableStorageStates;
        myRepairLockType = builder.myRepairLockType;
        myTimeBasedRunPolicy = builder.myTimeBasedRunPolicy;
    }

    /**
     * Create a scheduled repair job for the given node, table, and configuration.
     *
     * @param node The node to create the job for.
     * @param tableReference The table reference.
     * @param repairConfiguration The repair configuration.
     * @return A new ScheduledRepairJob.
     */
    public ScheduledRepairJob create(
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
                    .withRepairLockType(myRepairLockType)
                    .build();
        }
        else
        {
            LOG.info("Creating TableRepairJob for table {}.{} in node {}",
                    tableReference.getKeyspace(), tableReference.getTable(), node.getHostId());
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
                    .withRepairLockType(myRepairLockType)
                    .withNode(node)
                    .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                    .build();
        }
        job.refreshState();
        return job;
    }

    /**
     * Create a new Builder instance.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder for constructing {@link ScheduledRepairJobFactory}.
     */
    public static final class Builder
    {
        private TableRepairMetrics myTableRepairMetrics;
        private RepairHistoryService myRepairHistoryService;
        private RepairFaultReporter myFaultReporter;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private RepairStateFactory myRepairStateFactory;
        private ReplicationState myReplicationState;
        private CassandraMetrics myCassandraMetrics;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private TableStorageStates myTableStorageStates;
        private RepairLockType myRepairLockType;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairHistoryService(final RepairHistoryService repairHistoryService)
        {
            myRepairHistoryService = repairHistoryService;
            return this;
        }

        public Builder withFaultReporter(final RepairFaultReporter faultReporter)
        {
            myFaultReporter = faultReporter;
            return this;
        }

        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withRepairStateFactory(final RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        public Builder withCassandraMetrics(final CassandraMetrics cassandraMetrics)
        {
            myCassandraMetrics = cassandraMetrics;
            return this;
        }

        public Builder withRepairPolicies(final Collection<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies.addAll(repairPolicies);
            return this;
        }

        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        public Builder withTimeBasedRunPolicy(final TimeBasedRunPolicy timeBasedRunPolicy)
        {
            myTimeBasedRunPolicy = timeBasedRunPolicy;
            return this;
        }

        public ScheduledRepairJobFactory build()
        {
            return new ScheduledRepairJobFactory(this);
        }
    }
}
