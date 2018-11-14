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
import com.ericsson.bss.cassandra.ecchronos.core.LockCollection;
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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RepairGroup extends ScheduledTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairGroup.class);

    private static final int MAX_PARALLEL = 1;

    private static final String LOCK_METADATA_KEYSPACE = "keyspace";
    private static final String LOCK_METADATA_TABLE = "table";

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;

    public RepairGroup(int priority,
                       TableReference tableReference,
                       RepairConfiguration repairConfiguration,
                       RepairStateSnapshot repairStateSnapshot,
                       JmxProxyFactory jmxProxyFactory,
                       TableRepairMetrics tableRepairMetrics)
    {
        super(priority);

        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myJmxProxyFactory = jmxProxyFactory;
        myTableRepairMetrics = tableRepairMetrics;
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
        Collection<String> dataCenters = myRepairStateSnapshot.getRepairGroup().getDataCenters();
        for (String dc : dataCenters)
        {
            if (!lockFactory.sufficientNodesForLocking(dc, getResource(dc, 1)))
            {
                throw new LockException("Data center " + dc + " not lockable. Repair will be retried later.");
            }
        }

        if (dataCenters.isEmpty())
        {
            String msg = String.format("No data centers to lock for %s", this);
            LOG.warn(msg);
            throw new LockException(msg);
        }

        Collection<LockFactory.DistributedLock> locks = getDatacenterLocks(lockFactory, dataCenters);
        validateUniqueLock(lockFactory, locks, dataCenters);

        return new LockCollection(locks);
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

    /**
     * Validate that the table repair lock is unique for all data centers so that we don't start multiple repairs on the same table.
     *
     * @param lockFactory
     *            The lock factory
     * @param locks
     *            The locks currently owned
     * @param dataCenters
     *            The data centers to validate
     * @throws LockException
     *             Thrown if multiple repair locks were found for this table
     */
    private void validateUniqueLock(LockFactory lockFactory, Collection<LockFactory.DistributedLock> locks, Collection<String> dataCenters) throws LockException
    {
        for (String dataCenter : dataCenters)
        {
            if (getTableRepairLockCount(lockFactory, dataCenter) > MAX_PARALLEL)
            {
                releaseLocks(locks);
                try
                {
                    long sleepTime = new SecureRandom().nextInt(60) * 1000L;
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e)
                {
                    LOG.debug("Interrupted while sleeping in {}", this);
                    Thread.currentThread().interrupt();
                }
                String msg = String.format("%s is already in progress", this);
                LOG.warn(msg);
                throw new LockException(msg);
            }
        }
    }

    /**
     * Get a repair lock for this table for each data center.
     *
     * @param lockFactory
     *            The lock factory
     * @param datacenters
     *            The data centers to get repair locks in
     * @return The collection of locks acquired
     * @throws LockException
     *             Thrown when unable to get a lock in a data center
     */
    private Collection<LockFactory.DistributedLock> getDatacenterLocks(LockFactory lockFactory, Collection<String> datacenters) throws LockException
    {
        Collection<LockFactory.DistributedLock> locks = new ArrayList<>();

        for (String datacenter : datacenters)
        {
            try
            {
                locks.add(getLockForDatacenter(lockFactory, datacenter, myPriority));
            }
            catch (LockException e)
            {
                LOG.warn("{} - Unable to get lock for data center {}, releasing previously acquired locks", this, datacenter, e);
                releaseLocks(locks);
                throw e;
            }
        }

        return locks;
    }

    /**
     * Release the collection of acquired locks.
     *
     * @param locks
     *            The acquired locks.
     */
    private void releaseLocks(Collection<LockFactory.DistributedLock> locks)
    {
        for (LockFactory.DistributedLock lock : locks)
        {
            try
            {
                lock.close();
            }
            catch (Exception e)
            {
                LOG.error("Unable to release lock {} for {} ", lock, this, e);
            }
        }
    }

    /**
     * Try to get a repair lock for the specified data center.
     *
     * @param lockFactory
     *            The lock factory
     * @param dataCenter
     *            The data center to get the lock in
     * @param priority
     *            The priority this lock has
     * @return The acquired lock for the data center
     * @throws LockException
     *             Thrown when unable to get a lock in the data center
     */
    private LockFactory.DistributedLock getLockForDatacenter(LockFactory lockFactory, String dataCenter, int priority) throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(LOCK_METADATA_KEYSPACE, myTableReference.getKeyspace());
        metadata.put(LOCK_METADATA_TABLE, myTableReference.getTable());

        LockFactory.DistributedLock myLock;

        for (int i = 0; i < MAX_PARALLEL; i++)
        {
            String resource = getResource(dataCenter, i + 1);
            try
            {
                myLock = lockFactory.tryLock(dataCenter, resource, priority, metadata);

                if (myLock != null)
                {
                    return myLock;
                }
            }
            catch (LockException e)
            {
                LOG.warn("Lock ({} in data center {}) get error in {}", resource, dataCenter, this, e);
            }
        }

        String msg = String.format("Lock resources exhausted for %s", this);
        LOG.warn(msg);
        throw new LockException(msg);
    }

    /**
     * Get the total number of locks acquired for this table in the specified data center.
     *
     * @param lockFactory
     *            The lock factory
     * @param dataCenter
     *            The data center to check
     * @return The total number of locks acquired for this table
     */
    private int getTableRepairLockCount(LockFactory lockFactory, String dataCenter)
    {
        int ret = 0;
        for (int i = 0; i < MAX_PARALLEL; i++)
        {
            Map<String, String> lockMetadata = lockFactory.getLockMetadata(dataCenter, getResource(dataCenter, i + 1));

            if (lockMetadata != null
                    && myTableReference.getKeyspace().equals(lockMetadata.get(LOCK_METADATA_KEYSPACE))
                    && myTableReference.getTable().equals(lockMetadata.get(LOCK_METADATA_TABLE)))
            {
                ret++;
            }
        }

        return ret;
    }

    private String getResource(String dataCenter, int n)
    {
        return String.format("RepairResource-%s-%d", dataCenter, n);
    }
}
