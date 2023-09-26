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
import java.util.HashSet;
import java.util.LinkedHashSet;
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
    private final List<TableRepairPolicy> myRepairPolicies;
    private final UUID myJobId;
    private BigInteger myTokensPerRepair;
    private RepairHistory myRepairHistory;

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
        myRepairResourceFactory = Preconditions
                .checkNotNull(builder.myRepairResourceFactory, "Repair resource factory must be set");
        myRepairLockFactory = Preconditions
                .checkNotNull(builder.myRepairLockFactory, "Repair lock factory must be set");
        myRepairPolicies = new ArrayList<>(Preconditions
                .checkNotNull(builder.myRepairPolicies, "Repair policies must be set"));
        if (!myRepairConfiguration.getRepairType().equals(RepairOptions.RepairType.INCREMENTAL))
        {
            myRepairHistory = Preconditions
                    .checkNotNull(builder.myRepairHistory, "Repair history must be set");
        }
        if (myRepairConfiguration.getRepairType().equals(RepairOptions.RepairType.VNODE))
        {
            myTokensPerRepair = Preconditions
                    .checkNotNull(builder.myTokensPerRepair, "Tokens per repair must be set");
        }
        myJobId = Preconditions
                .checkNotNull(builder.myJobId, "Job id must be set");
    }

    /**
     * Executes the repair tasks this repair group is responsible for. Repair tasks can succeed or fail. Repair
     * tasks blocked by run policy are counted as failed.
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
     * Get lock for the keyspace and table.
     *
     * @param lockFactory The lock factory to use.
     * @return LockFactory.DistributedLock
     * @throws LockException Lock factory unable to get a lock.
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
        return String.format("%s repair group of %s", myRepairConfiguration.getRepairType(), myTableReference);
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
        if (myRepairConfiguration.getRepairType().equals(RepairOptions.RepairType.INCREMENTAL))
        {
            tasks.add(new IncrementalRepairTask(myJmxProxyFactory, myTableReference,
                    myRepairConfiguration, myTableRepairMetrics));
        }
        else if (myRepairConfiguration.getRepairType().equals(RepairOptions.RepairType.PARALLEL_VNODE))
        {
            Set<LongTokenRange> combinedRanges = new LinkedHashSet<>();
            myReplicaRepairGroup.iterator().forEachRemaining(combinedRanges::add);
            tasks.add(new VnodeRepairTask(myJmxProxyFactory, myTableReference, myRepairConfiguration,
                    myTableRepairMetrics, myRepairHistory, combinedRanges,
                    new HashSet<>(myReplicaRepairGroup.getReplicas()), myJobId));
        }
        else
        {
            for (LongTokenRange range : myReplicaRepairGroup)
            {
                for (LongTokenRange subRange : new TokenSubRangeUtil(range).generateSubRanges(myTokensPerRepair))
                {
                    tasks.add(new VnodeRepairTask(myJmxProxyFactory, myTableReference, myRepairConfiguration,
                            myTableRepairMetrics, myRepairHistory, Collections.singleton(subRange),
                            new HashSet<>(myReplicaRepairGroup.getReplicas()), myJobId));
                }
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
        private TableReference myTableReference;
        private RepairConfiguration myRepairConfiguration;
        private ReplicaRepairGroup myReplicaRepairGroup;
        private JmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private RepairResourceFactory myRepairResourceFactory;
        private RepairLockFactory myRepairLockFactory;
        private List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private BigInteger myTokensPerRepair = LongTokenRange.FULL_RANGE;
        private RepairHistory myRepairHistory;
        private UUID myJobId;

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
        public Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
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
         * Build with repair resource factory.
         *
         * @param repairResourceFactory Repair resource factory.
         * @return Builder
         */
        public Builder withRepairResourceFactory(final RepairResourceFactory repairResourceFactory)
        {
            myRepairResourceFactory = repairResourceFactory;
            return this;
        }

        /**
         * Build with repair lock factory.
         *
         * @param repairLockFactory Repair lock factory.
         * @return Builder
         */
        public Builder withRepairLockFactory(final RepairLockFactory repairLockFactory)
        {
            myRepairLockFactory = repairLockFactory;
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
         * Build with tokens per repair.
         *
         * @param tokensPerRepair Tokens per repair.
         * @return Builder
         */
        public Builder withTokensPerRepair(final BigInteger tokensPerRepair)
        {
            myTokensPerRepair = tokensPerRepair;
            return this;
        }

        /**
         * Build with repair history.
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
         * Build with job id.
         *
         * @param jobId Job id.
         * @return Builder
         */
        public Builder withJobId(final UUID jobId)
        {
            myJobId = jobId;
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
