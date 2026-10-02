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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Factory responsible for creating one multi-node {@link UnifiedTableRepairJob} per table and configuration.
 * <p>
 * Mirrors the legacy {@code ScheduledRepairJobFactory} but is dedicated to the {@code UNIFIED_VNODE} path:
 * for the given set of nodes it builds a per-node {@link RepairState} (via {@link RepairStateFactory}) wrapped
 * in a {@link NodeRepairState}, and assembles them into a single consolidated job.
 */
public final class UnifiedRepairJobFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedRepairJobFactory.class);

    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairHistoryService myRepairHistoryService;
    private final RepairFaultReporter myFaultReporter;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final RepairStateFactory myRepairStateFactory;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableStorageStates myTableStorageStates;
    private final RepairLockType myRepairLockType;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    private UnifiedRepairJobFactory(final Builder builder)
    {
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myRepairHistoryService = builder.myRepairHistoryService;
        myFaultReporter = builder.myFaultReporter;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myRepairStateFactory = builder.myRepairStateFactory;
        myRepairPolicies = new ArrayList<>(builder.myRepairPolicies);
        myTableStorageStates = builder.myTableStorageStates;
        myRepairLockType = builder.myRepairLockType;
        myTimeBasedRunPolicy = builder.myTimeBasedRunPolicy;
    }

    /**
     * Create a consolidated multi-node repair job for the given nodes, table, and configuration.
     *
     * @param nodes the nodes that replicate the table.
     * @param tableReference the table.
     * @param repairConfiguration the (vnode) repair configuration.
     * @return a new {@link UnifiedTableRepairJob}.
     */
    public UnifiedTableRepairJob createMultiNode(
            final Collection<Node> nodes,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(repairConfiguration.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withBackoff(repairConfiguration.getBackoffInMs(), TimeUnit.MILLISECONDS)
                .withPriorityGranularity(repairConfiguration.getPriorityGranularityUnit())
                .build();

        UnifiedTableRepairJob.Builder builder = new UnifiedTableRepairJob.Builder()
                .withConfiguration(configuration)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(tableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .withTableStorageStates(myTableStorageStates)
                .withRepairPolices(myRepairPolicies)
                .withRepairHistory(myRepairHistoryService)
                .withRepairLockType(myRepairLockType)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy);

        for (Node node : nodes)
        {
            AlarmPostUpdateHook alarmPostUpdateHook =
                    new AlarmPostUpdateHook(tableReference, repairConfiguration, myFaultReporter);
            RepairState repairState =
                    myRepairStateFactory.create(node, tableReference, repairConfiguration, alarmPostUpdateHook);
            builder.withNodeRepairState(new NodeRepairState(node, repairState));
        }

        LOG.debug("Creating UnifiedTableRepairJob for table {}.{} across {} nodes",
                tableReference.getKeyspace(), tableReference.getTable(), nodes.size());
        UnifiedTableRepairJob job = builder.build();
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
     * Builder for constructing {@link UnifiedRepairJobFactory}.
     */
    public static final class Builder
    {
        private TableRepairMetrics myTableRepairMetrics;
        private RepairHistoryService myRepairHistoryService;
        private RepairFaultReporter myFaultReporter;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private RepairStateFactory myRepairStateFactory;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private TableStorageStates myTableStorageStates;
        private RepairLockType myRepairLockType;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Set the table repair metrics.
         *
         * @param tableRepairMetrics the table repair metrics.
         * @return this builder.
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Set the repair history service.
         *
         * @param repairHistoryService the repair history service.
         * @return this builder.
         */
        public Builder withRepairHistoryService(final RepairHistoryService repairHistoryService)
        {
            myRepairHistoryService = repairHistoryService;
            return this;
        }

        /**
         * Set the fault reporter.
         *
         * @param faultReporter the fault reporter.
         * @return this builder.
         */
        public Builder withFaultReporter(final RepairFaultReporter faultReporter)
        {
            myFaultReporter = faultReporter;
            return this;
        }

        /**
         * Set the JMX proxy factory.
         *
         * @param jmxProxyFactory the JMX proxy factory.
         * @return this builder.
         */
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Set the repair state factory.
         *
         * @param repairStateFactory the repair state factory.
         * @return this builder.
         */
        public Builder withRepairStateFactory(final RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        /**
         * Set the repair policies.
         *
         * @param repairPolicies the collection of repair policies.
         * @return this builder.
         */
        public Builder withRepairPolicies(final Collection<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies.addAll(repairPolicies);
            return this;
        }

        /**
         * Set the table storage states.
         *
         * @param tableStorageStates the table storage states.
         * @return this builder.
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        /**
         * Set the repair lock type.
         *
         * @param repairLockType the repair lock type.
         * @return this builder.
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Set the time-based run policy.
         *
         * @param timeBasedRunPolicy the time-based run policy.
         * @return this builder.
         */
        public Builder withTimeBasedRunPolicy(final TimeBasedRunPolicy timeBasedRunPolicy)
        {
            myTimeBasedRunPolicy = timeBasedRunPolicy;
            return this;
        }

        /**
         * Build the {@link UnifiedRepairJobFactory}.
         *
         * @return a new UnifiedRepairJobFactory instance.
         */
        public UnifiedRepairJobFactory build()
        {
            return new UnifiedRepairJobFactory(this);
        }
    }
}
