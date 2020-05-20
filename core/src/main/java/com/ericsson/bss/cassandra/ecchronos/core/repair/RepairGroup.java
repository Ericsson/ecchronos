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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final List<TableRepairPolicy> myRepairPolicies;

    public RepairGroup(int priority,
                       TableReference tableReference,
                       RepairConfiguration repairConfiguration,
                       ReplicaRepairGroup replicaRepairGroup,
                       JmxProxyFactory jmxProxyFactory,
                       TableRepairMetrics tableRepairMetrics,
                       RepairResourceFactory repairResourceFactory,
                       RepairLockFactory repairLockFactory)
    {
        this(priority, tableReference, repairConfiguration, replicaRepairGroup, jmxProxyFactory,
                tableRepairMetrics, repairResourceFactory, repairLockFactory, Collections.emptyList());
    }

    public RepairGroup(int priority,
            TableReference tableReference,
            RepairConfiguration repairConfiguration,
            ReplicaRepairGroup replicaRepairGroup,
            JmxProxyFactory jmxProxyFactory,
            TableRepairMetrics tableRepairMetrics,
            RepairResourceFactory repairResourceFactory,
            RepairLockFactory repairLockFactory,
            List<TableRepairPolicy> tableRepairPolicies)
    {
        super(priority);

        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myReplicaRepairGroup = replicaRepairGroup;
        myJmxProxyFactory = jmxProxyFactory;
        myTableRepairMetrics = tableRepairMetrics;
        myRepairResourceFactory = repairResourceFactory;
        myRepairLockFactory = repairLockFactory;
        myRepairPolicies = new ArrayList<>(tableRepairPolicies);
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
        for (TableRepairPolicy repairPolicy : myRepairPolicies)
        {
            if (!repairPolicy.shouldRun(myTableReference))
            {
                return false;
            }
        }

        return true;
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
            builder.withTokenRanges(Collections.singletonList(range));

            tasks.add(builder.build());
        }

        return tasks;
    }
}
