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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RepairGroup extends ScheduledTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairGroup.class);

    private static final String LOCK_METADATA_KEYSPACE = "keyspace";
    private static final String LOCK_METADATA_TABLE = "table";

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairResourceFactory myRepairResourceFactory;
    private final RepairLockFactory myRepairLockFactory;

    public RepairGroup(int priority,
                       TableReference tableReference,
                       RepairConfiguration repairConfiguration,
                       RepairStateSnapshot repairStateSnapshot,
                       JmxProxyFactory jmxProxyFactory,
                       TableRepairMetrics tableRepairMetrics,
                       RepairResourceFactory repairResourceFactory,
                       RepairLockFactory repairLockFactory)
    {
        super(priority);

        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myJmxProxyFactory = jmxProxyFactory;
        myTableRepairMetrics = tableRepairMetrics;
        myRepairResourceFactory = repairResourceFactory;
        myRepairLockFactory = repairLockFactory;
    }

    @Override
    public boolean preValidate()
    {
        return myRepairStateSnapshot.canRepair();
    }

    @Override
    public boolean execute()
    {
        LOG.info("Table {} running repair job {}", myTableReference, myRepairStateSnapshot.getRepairGroup());
        boolean successful = true;

        for (RepairTask repairTask : getRepairTasks())
        {
            try
            {
                repairTask.execute();
            }
            catch (ScheduledJobException e)
            {
                LOG.warn("Encountered issue when running repair task {}", repairTask, e);
                successful = false;
            }
            finally
            {
                repairTask.cleanup();
            }
        }

        return successful;
    }

    @Override
    public LockFactory.DistributedLock getLock(LockFactory lockFactory) throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(LOCK_METADATA_KEYSPACE, myTableReference.getKeyspace());
        metadata.put(LOCK_METADATA_TABLE, myTableReference.getTable());

        Set<RepairResource> repairResources = myRepairResourceFactory.getRepairResources(myRepairStateSnapshot.getRepairGroup());
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

        boolean vnodeRepair = RepairOptions.RepairType.VNODE.equals(myRepairConfiguration.getRepairType());

        RepairTask.Builder builder = new RepairTask.Builder()
                .withJMXProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withVnodeRepair(vnodeRepair)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration);

        if (vnodeRepair)
        {
            ReplicaRepairGroup replicaRepairGroup = myRepairStateSnapshot.getRepairGroup();

            for (LongTokenRange range : replicaRepairGroup)
            {
                builder.withTokenRanges(Collections.singletonList(range))
                        .withReplicas(replicaRepairGroup.getReplicas());

                tasks.add(builder.build());
            }
        }
        else
        {
            ReplicaRepairGroup replicaRepairGroup = myRepairStateSnapshot.getRepairGroup();

            Set<Host> replicas = replicaRepairGroup.getReplicas();

            if (!replicas.isEmpty())
            {
                builder.withReplicas(replicas);
            }

            builder.withTokenRanges(replicaRepairGroup.getVnodes());
            tasks.add(builder.build());
        }

        return tasks;
    }
}
