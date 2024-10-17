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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to construct repair groups.
 */
public class RepairGroup extends ScheduledTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairGroup.class);

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final ReplicaRepairGroup myReplicaRepairGroup;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final List<TableRepairPolicy> myRepairPolicies;

    /**
     * Constructs an IncrementalRepairTask for a specific node and table.
     *
     * @param priority the priority for job creation.
     * @param builder the Builder to construct RepairGroup.
     */
    public RepairGroup(final int priority, final Builder builder)
    {
        super(priority);
        myTableReference = Preconditions
                .checkNotNull(builder.myTableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions
                .checkNotNull(builder.myRepairConfiguration, "Repair configuration must be set");
        myReplicaRepairGroup = Preconditions
                .checkNotNull(builder.myReplicaRepairGroup, "Replica repair group must be set");
        myJmxProxyFactory = Preconditions
                .checkNotNull(builder.myJmxProxyFactory, "Jmx proxy factory must be set");
        myTableRepairMetrics = Preconditions
                .checkNotNull(builder.myTableRepairMetrics, "Table repair metrics must be set");
        myRepairPolicies = new ArrayList<>(Preconditions
                .checkNotNull(builder.myRepairPolicies, "Repair policies must be set"));
    }

    /**
     * Executes the repair tasks this repair group is responsible for. Repair tasks can succeed or fail. Repair
     * tasks blocked by run policy are counted as failed.
     *
     * @return boolean
     */
    @Override
    public boolean execute(final UUID nodeID)
    {
        LOG.debug("Table {} running repair job {}", myTableReference, myReplicaRepairGroup);
        boolean successful = true;

        for (RepairTask repairTask : getRepairTasks(nodeID))
        {
            if (!shouldContinue())
            {
                LOG.info("Repair of {} was stopped by policy, will continue later", this);
                successful = false;
                break;
            }
            try
            {
                repairTask.execute();
            }
            catch (ScheduledJobException e)
            {
                LOG.warn("Encountered issue when running repair task {}, {}", repairTask, e.getMessage());
                LOG.debug("", e);
                successful = false;
                if (e.getCause() instanceof InterruptedException)
                {
                    LOG.info("{} thread was interrupted", this);
                    break;
                }
            }
            finally
            {
                repairTask.cleanup();
            }
        }

        return successful;
    }

    private boolean shouldContinue()
    {
        return myRepairPolicies.stream().allMatch(repairPolicy -> repairPolicy.shouldRun(myTableReference));
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("%s repair group of %s", myRepairConfiguration.getRepairType(), myTableReference);
    }

    /**
     * Get repair tasks.
     *
     * @param nodeID the Node id.
     * @return a Collection of RepairTask
     */
    @VisibleForTesting
    public Collection<RepairTask> getRepairTasks(final UUID nodeID)
    {
        Collection<RepairTask> tasks = new ArrayList<>();
        tasks.add(new IncrementalRepairTask(
                nodeID,
                myJmxProxyFactory,
                myTableReference,
                myRepairConfiguration,
                myTableRepairMetrics));

        return tasks;
    }

    /**
     * Create instance of Builder to construct RepairGroup.
     *
     * @return Builder
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * Builder used to construct RepairGroup.
     */
    public static class Builder
    {
        private TableReference myTableReference;
        private RepairConfiguration myRepairConfiguration;
        private ReplicaRepairGroup myReplicaRepairGroup;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();


        /**
         * Build with table reference.
         *
         * @param tableReference Table reference.
         * @return Builder
         */
        public Builder withTableReference(final TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param repairConfiguration Repair configuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Build with replica repair group.
         *
         * @param replicaRepairGroup Replica repair group.
         * @return Builder
         */
        public Builder withReplicaRepairGroup(final ReplicaRepairGroup replicaRepairGroup)
        {
            myReplicaRepairGroup = replicaRepairGroup;
            return this;
        }

        /**
         * Build with JMX proxy factory.
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
         * Build with table repair metrics.
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
         * Build with repair policies.
         *
         * @param repairPolicies Repair policies.
         * @return Builder
         */
        public Builder withRepairPolicies(final List<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies = repairPolicies;
            return this;
        }

        /**
         * Build repair group.
         *
         * @param priority The priority.
         * @return RepairGroup
         */
        public RepairGroup build(final int priority)
        {
            return new RepairGroup(priority, this);
        }
    }
}

