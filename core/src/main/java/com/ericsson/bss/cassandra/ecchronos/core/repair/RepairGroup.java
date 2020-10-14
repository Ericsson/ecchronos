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
import java.util.*;

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

    public RepairGroup(int priority, Builder builder)
    {
        super(priority);
        myTableReference = Preconditions.checkNotNull(builder.tableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions.checkNotNull(builder.repairConfiguration, "Repair configuration must be set");
        myReplicaRepairGroup = Preconditions.checkNotNull(builder.replicaRepairGroup, "Replica repair group must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(builder.jmxProxyFactory, "Jmx proxy factory must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(builder.tableRepairMetrics, "Table repair metrics must be set");
        myRepairResourceFactory = Preconditions.checkNotNull(builder.repairResourceFactory, "Repair resource factory must be set");
        myRepairLockFactory = Preconditions.checkNotNull(builder.repairLockFactory, "Repair lock factory must be set");
        myTokensPerRepair = Preconditions.checkNotNull(builder.tokensPerRepair, "Tokens per repair must be set");
        myRepairPolicies = new ArrayList<>(Preconditions.checkNotNull(builder.repairPolicies, "Repair policies must be set"));
    }

    @Override
    public boolean execute()
    {
        LOG.info("Table {} running repair job {}", myTableReference, myReplicaRepairGroup);
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
                LOG.warn("Encountered issue when running repair task {}", repairTask, e);
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

    @Override
    public LockFactory.DistributedLock getLock(LockFactory lockFactory) throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(LOCK_METADATA_KEYSPACE, myTableReference.getKeyspace());
        metadata.put(LOCK_METADATA_TABLE, myTableReference.getTable());

        Set<RepairResource> repairResources = myRepairResourceFactory.getRepairResources(myReplicaRepairGroup);
        return myRepairLockFactory.getLock(lockFactory, repairResources, metadata, myPriority);
    }

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    @VisibleForTesting
    Collection<RepairTask> getRepairTasks()
    {
        Collection<RepairTask> tasks = new ArrayList<>();

        RepairTask.Builder builder = new RepairTask.Builder()
                .withJMXProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withReplicas(myReplicaRepairGroup.getReplicas());

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

        public Builder withTableReference(TableReference tableReference)
        {
            this.tableReference = tableReference;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withReplicaRepairGroup(ReplicaRepairGroup replicaRepairGroup)
        {
            this.replicaRepairGroup = replicaRepairGroup;
            return this;
        }

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            this.jmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairResourceFactory(RepairResourceFactory repairResourceFactory)
        {
            this.repairResourceFactory = repairResourceFactory;
            return this;
        }

        public Builder withRepairLockFactory(RepairLockFactory repairLockFactory)
        {
            this.repairLockFactory = repairLockFactory;
            return this;
        }

        public Builder withRepairPolicies(List<TableRepairPolicy> repairPolicies)
        {
            this.repairPolicies = repairPolicies;
            return this;
        }

        public Builder withTokensPerRepair(BigInteger tokensPerRepair)
        {
            this.tokensPerRepair = tokensPerRepair;
            return this;
        }

        public RepairGroup build(int priority)
        {
            return new RepairGroup(priority, this);
        }
    }
}
