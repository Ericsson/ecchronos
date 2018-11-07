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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.LockCollection;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;
import com.google.common.annotations.VisibleForTesting;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this node if all the ranges this node
 * is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
public class ScheduledRepairJob extends ScheduledJob
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledRepairJob.class);

    private static final int MAX_PARALLEL = 1;

    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairState myRepairState;
    private final RepairFaultReporter myFaultReporter;
    private final RepairConfiguration myRepairConfiguration;

    private final TableRepairMetrics myTableRepairMetrics;

    private final AtomicReference<Clock> myClock = new AtomicReference<>(Clock.DEFAULT);

    ScheduledRepairJob(Builder builder)
    {
        super(builder.configuration);

        myTableReference = builder.tableReference;
        myJmxProxyFactory = builder.jmxProxyFactory;
        myRepairState = builder.repairState;
        myFaultReporter = builder.faultReporter;
        myTableRepairMetrics = builder.tableRepairMetrics;
        myRepairConfiguration = builder.repairConfiguration;
    }

    public TableReference getTableReference()
    {
        return myTableReference;
    }

    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    @Override
    public boolean preValidate()
    {
        try
        {
            myRepairState.update();
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }

        long lastRepaired = myRepairState.lastRepairedAt();

        if (lastRepaired != -1)
        {
            sendOrCeaseAlarm(lastRepaired, myRepairState.getLocalRangesForRepair());
        }

        return myRepairState.canRepair();
    }

    @Override
    public void postExecute(boolean successful)
    {
        boolean realSuccessful = false;

        try
        {
            myRepairState.update();

            long lastRepaired = myRepairState.lastRepairedAt();

            if (lastRepaired != -1)
            {
                sendOrCeaseAlarm(lastRepaired, myRepairState.getLocalRangesForRepair());
            }

            realSuccessful = !myRepairState.canRepair();
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }

        super.postExecute(realSuccessful);
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return myRepairState.lastRepairedAt();
    }

    private void raiseAlarm(FaultCode faultCode)
    {
        Map<String, Object> faultData = new HashMap<>();
        faultData.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        faultData.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        myFaultReporter.raise(faultCode, faultData);
    }

    private void ceaseWarningAlarm()
    {
        Map<String, Object> ceaseData = new HashMap<>();
        ceaseData.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        ceaseData.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        myFaultReporter.cease(FaultCode.REPAIR_WARNING, ceaseData);
    }

    private void sendOrCeaseAlarm(long lastRepaired, Collection<LongTokenRange> nonRepairedRanges)
    {
        long msSinceLastRepair = myClock.get().getTime() - lastRepaired;

        FaultCode faultCode = null;

        if (msSinceLastRepair >= myRepairConfiguration.getRepairErrorTimeInMs())
        {
            faultCode = FaultCode.REPAIR_ERROR;
        }
        else if (msSinceLastRepair >= myRepairConfiguration.getRepairWarningTimeInMs())
        {
            faultCode = FaultCode.REPAIR_WARNING;
        }

        if (faultCode != null)
        {
            raiseAlarm(faultCode);
        }
        else if (nonRepairedRanges.isEmpty())
        {
            ceaseWarningAlarm();
        }
    }

    @Override
    public boolean runnable()
    {
        if (super.runnable())
        {
            try
            {
                myRepairState.update();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to check repair history, {}", this, e);
            }
        }

        long lastRepaired = myRepairState.lastRepairedAt();

        if (lastRepaired != -1)
        {
            sendOrCeaseAlarm(lastRepaired, myRepairState.getLocalRangesForRepair());
        }

        return myRepairState.canRepair() && super.runnable();
    }

    @Override
    public LockFactory.DistributedLock getLock(LockFactory lockFactory) throws LockException
    {
        Collection<String> dataCenters = myRepairState.getDatacentersForRepair();
        Iterator<String> dataCentersIterator = dataCenters.iterator();
        while (dataCentersIterator.hasNext())
        {
            final String dc = dataCentersIterator.next();
            if (!lockFactory.sufficientNodesForLocking(dc, getResource(dc, 1)))
            {
                LOG.warn("Data center {} not lockable. Repair will be retried later", dc);
                raiseAlarm(FaultCode.REPAIR_WARNING);
                dataCentersIterator.remove();
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
    public Iterator<ScheduledTask> iterator()
    {
        return getRepairTasks().iterator();
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

        int priority = getRealPriority();

        for (String datacenter : datacenters)
        {
            try
            {
                locks.add(getLockForDatacenter(lockFactory, datacenter, priority));
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
            if (getTableRepairLockCount(lockFactory, dataCenter) > 1)
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
        metadata.put("keyspace", myTableReference.getKeyspace());
        metadata.put("table", myTableReference.getTable());

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
                    && myTableReference.getKeyspace().equals(lockMetadata.get("keyspace"))
                    && myTableReference.getTable().equals(lockMetadata.get("table")))
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

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    private Collection<ScheduledTask> getRepairTasks()
    {
        Collection<ScheduledTask> tasks = new ArrayList<>();

        boolean vnodeRepair = RepairOptions.RepairType.VNODE.equals(myRepairConfiguration.getRepairType());

        RepairTask.Builder builder = new RepairTask.Builder()
                .withJMXProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withVnodeRepair(vnodeRepair)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration);

        if (vnodeRepair)
        {
            Map<LongTokenRange, Collection<Host>> rangeToReplicaMap = myRepairState.getRangeToReplicas();

            for (LongTokenRange range : myRepairState.getLocalRangesForRepair())
            {
                Collection<Host> replicas = rangeToReplicaMap.get(range);

                if (replicas != null)
                {
                    builder.withTokenRanges(Collections.singletonList(range));
                    builder.withReplicas(replicas);
                    tasks.add(builder.build());
                }
            }
        }
        else
        {
            Set<Host> replicas = myRepairState.getReplicas();

            if (!replicas.isEmpty())
            {
                builder.withReplicas(replicas);
            }

            builder.withTokenRanges(myRepairState.getAllRanges());
            tasks.add(builder.build());
        }

        return tasks;
    }

    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(7, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private RepairState repairState;
        private RepairFaultReporter faultReporter;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;

        public Builder withConfiguration(Configuration configuration)
        {
            this.configuration = configuration;
            return this;
        }

        public Builder withTableReference(TableReference tableReference)
        {
            this.tableReference = tableReference;
            return this;
        }

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            this.jmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withRepairState(RepairState repairState)
        {
            this.repairState = repairState;
            return this;
        }

        public Builder withFaultReporter(RepairFaultReporter faultReporter)
        {
            this.faultReporter = faultReporter;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public ScheduledRepairJob build()
        {
            if (tableReference == null)
            {
                throw new IllegalArgumentException("Table reference cannot be null");
            }
            if (faultReporter == null)
            {
                throw new IllegalArgumentException("Fault reporter cannot be null");
            }
            if (jmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX Proxy factory cannot be null");
            }
            if (tableRepairMetrics == null)
            {
                throw new IllegalArgumentException("Metric interface not set");
            }
            return new ScheduledRepairJob(this);
        }
    }

    @VisibleForTesting
    void setClock(Clock clock)
    {
        myClock.set(clock);
    }
}
