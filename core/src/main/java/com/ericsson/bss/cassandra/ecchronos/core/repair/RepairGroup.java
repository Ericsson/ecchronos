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
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TokenSubRangeUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RepairGroup extends ScheduledTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairGroup.class);

    private static final String LOCK_METADATA_KEYSPACE = "keyspace";
    private static final String LOCK_METADATA_TABLE = "table";

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final ReplicaRepairGroup myReplicaRepairGroup;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairResourceFactory myRepairResourceFactory;
    private final RepairLockFactory myRepairLockFactory;
    private final BigInteger myTokensPerRepair;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final RepairHistory myRepairHistory;
    private final UUID myJobId;

    public RepairGroup(final int priority, final Builder builder)
    {
        super(priority);
        myTableReference = Preconditions
                .checkNotNull(builder.tableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions
                .checkNotNull(builder.repairConfiguration, "Repair configuration must be set");
        myReplicaRepairGroup = Preconditions
                .checkNotNull(builder.replicaRepairGroup, "Replica repair group must be set");
        myJmxProxyFactory = Preconditions
                .checkNotNull(builder.jmxProxyFactory, "Jmx proxy factory must be set");
        myTableRepairMetrics = Preconditions
                .checkNotNull(builder.tableRepairMetrics, "Table repair metrics must be set");
        myRepairResourceFactory = Preconditions
                .checkNotNull(builder.repairResourceFactory, "Repair resource factory must be set");
        myRepairLockFactory = Preconditions
                .checkNotNull(builder.repairLockFactory, "Repair lock factory must be set");
        myTokensPerRepair = Preconditions
                .checkNotNull(builder.tokensPerRepair, "Tokens per repair must be set");
        myRepairPolicies = new ArrayList<>(Preconditions
                .checkNotNull(builder.repairPolicies, "Repair policies must be set"));
        myRepairHistory = Preconditions
                .checkNotNull(builder.repairHistory, "Repair history must be set");
        myJobId = Preconditions
                .checkNotNull(builder.jobId, "Job id must be set");
    }

    /**
     * Execute repair job.
     *
     * @return boolean
     */
    @Override
    public boolean execute()
    {
        LOG.debug("Table {} running repair job {}", myTableReference, myReplicaRepairGroup);
        boolean successful = true;

        for (RepairTask repairTask : getRepairTasks())
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
                myTableRepairMetrics.succeededRepairTask(myTableReference);
            }
            catch (ScheduledJobException e)
            {
                LOG.warn("Encountered issue when running repair task {}", repairTask, e);
                successful = false;
                myTableRepairMetrics.failedRepairTask(myTableReference);
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
     * Get lock for the table.
     *
     * @param lockFactory The lock factory to use.
     * @return LockFactory.DistributedLock
     * @throws LockException
     */
    @Override
    public LockFactory.DistributedLock getLock(final LockFactory lockFactory) throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(LOCK_METADATA_KEYSPACE, myTableReference.getKeyspace());
        metadata.put(LOCK_METADATA_TABLE, myTableReference.getTable());

        Set<RepairResource> repairResources = myRepairResourceFactory.getRepairResources(myReplicaRepairGroup);
        return myRepairLockFactory.getLock(lockFactory, repairResources, metadata, myPriority);
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    /**
     * Get repair tasks.
     *
     * @return Collection<RepairTask>
     */
    @VisibleForTesting
    Collection<RepairTask> getRepairTasks()
    {
        Collection<RepairTask> tasks = new ArrayList<>();

        RepairTask.Builder builder = new RepairTask.Builder()
                .withJMXProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withReplicas(myReplicaRepairGroup.getReplicas())
                .withRepairHistory(myRepairHistory)
                .withJobId(myJobId);

        for (LongTokenRange range : myReplicaRepairGroup)
        {
            for (LongTokenRange subRange : new TokenSubRangeUtil(range).generateSubRanges(myTokensPerRepair))
            {
                builder.withTokenRanges(Collections.singletonList(subRange));
                tasks.add(builder.build());
            }
        }

        return tasks;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private List<TableRepairPolicy> repairPolicies = new ArrayList<>();
        private BigInteger tokensPerRepair = LongTokenRange.FULL_RANGE;

        private TableReference tableReference;
        private RepairConfiguration repairConfiguration;
        private ReplicaRepairGroup replicaRepairGroup;
        private JmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics;
        private RepairResourceFactory repairResourceFactory;
        private RepairLockFactory repairLockFactory;
        private RepairHistory repairHistory;
        private UUID jobId;

        /**
         * Build with table reference.
         *
         * @return Builder
         */
        public Builder withTableReference(final TableReference theTableReference)
        {
            this.tableReference = theTableReference;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration theRepairConfiguration)
        {
            this.repairConfiguration = theRepairConfiguration;
            return this;
        }

        /**
         * Build with replica repair group.
         *
         * @return Builder
         */
        public Builder withReplicaRepairGroup(final ReplicaRepairGroup theReplicaRepairGroup)
        {
            this.replicaRepairGroup = theReplicaRepairGroup;
            return this;
        }

        /**
         * Build with JMX proxy factory.
         *
         * @return Builder
         */
        public Builder withJmxProxyFactory(final JmxProxyFactory theJMXProxyFactory)
        {
            this.jmxProxyFactory = theJMXProxyFactory;
            return this;
        }

        /**
         * Build with table repair metrics.
         *
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics theTableRepairMetrics)
        {
            this.tableRepairMetrics = theTableRepairMetrics;
            return this;
        }

        /**
         * Build with repair resource factory.
         *
         * @return Builder
         */
        public Builder withRepairResourceFactory(final RepairResourceFactory theRepairResourceFactory)
        {
            this.repairResourceFactory = theRepairResourceFactory;
            return this;
        }

        /**
         * Build with repair lock repair lock factory.
         *
         * @return Builder
         */
        public Builder withRepairLockFactory(final RepairLockFactory theRepairLockFactory)
        {
            this.repairLockFactory = theRepairLockFactory;
            return this;
        }

        /**
         * Build with repair policies.
         *
         * @return Builder
         */
        public Builder withRepairPolicies(final List<TableRepairPolicy> theRepairPolicies)
        {
            this.repairPolicies = theRepairPolicies;
            return this;
        }

        /**
         * Build with tokens per repair.
         *
         * @return Builder
         */
        public Builder withTokensPerRepair(final BigInteger theTokensPerRepair)
        {
            this.tokensPerRepair = theTokensPerRepair;
            return this;
        }

        /**
         * Build with repair history.
         *
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory theRepairHistory)
        {
            this.repairHistory = theRepairHistory;
            return this;
        }

        /**
         * Build with job id.
         *
         * @return Builder
         */
        public Builder withJobId(final UUID theJobId)
        {
            this.jobId = theJobId;
            return this;
        }

        /**
         * Build repair group.
         *
         * @param priority
         * @return RepairGroup
         */
        public RepairGroup build(final int priority)
        {
            return new RepairGroup(priority, this);
        }
    }
}
