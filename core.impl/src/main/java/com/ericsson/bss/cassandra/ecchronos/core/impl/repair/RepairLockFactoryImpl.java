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

import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.LockCollection;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairLockFactoryImpl implements RepairLockFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairLockFactoryImpl.class);
    private static final int DEFAULT_LOCKS_PER_RESOURCE = 1;
    private static final AtomicInteger CONFIGURED_LOCKS_PER_RESOURCE = new AtomicInteger(DEFAULT_LOCKS_PER_RESOURCE);

    private final ThreadLocal<Integer> myLockCounter = ThreadLocal.withInitial(() -> 0);

    /**
     * Configure the global locks per resource value.
     * Should be called once at application startup before any repair jobs are created.
     *
     * @param locksPerResource the number of concurrent locks per resource.
     */
    public static void configure(final int locksPerResource)
    {
        if (locksPerResource < DEFAULT_LOCKS_PER_RESOURCE)
        {
            throw new IllegalArgumentException("locksPerResource must be at least 1");
        }
        CONFIGURED_LOCKS_PER_RESOURCE.set(locksPerResource);
        LOG.info("RepairLockFactory configured with {} locks per resource", locksPerResource);
    }

    private int getLocksPerResource()
    {
        return CONFIGURED_LOCKS_PER_RESOURCE.get();
    }

    @Override
    public final LockFactory.DistributedLock getLock(final LockFactory lockFactory,
                                                     final Set<RepairResource> repairResources,
                                                     final Map<String, String> metadata,
                                                     final int priority,
                                                     final UUID nodeId) throws LockException
    {
        int locksPerResource = getLocksPerResource();
        for (RepairResource repairResource : repairResources)
        {
            if (!lockFactory.sufficientNodesForLocking(repairResource.getResourceName(locksPerResource)))
            {
                throw new LockException(repairResource + " not lockable. Repair will be retried later.");
            }
        }

        if (repairResources.isEmpty())
        {
            String msg = String.format("No datacenters to lock for %s", this);
            LOG.warn(msg);
            throw new LockException(msg);
        }

        validateNoCachedFailures(nodeId, lockFactory, repairResources);

        Collection<LockFactory.DistributedLock> locks = getRepairResourceLocks(lockFactory,
                repairResources,
                metadata,
                priority,
                nodeId);

        return new LockCollection(locks);
    }

    private void validateNoCachedFailures(final UUID nodeId, final LockFactory lockFactory, final Set<RepairResource> repairResources)
                                                                                                                    throws LockException
    {
        int locksPerResource = getLocksPerResource();
        for (RepairResource repairResource : repairResources)
        {
            Optional<LockException> cachedException = lockFactory.getCachedFailure(nodeId, repairResource.getDataCenter(),
                    repairResource.getResourceName(locksPerResource));
            if (cachedException.isPresent())
            {
                LockException e = cachedException.get();
                LOG.debug("Found cached locking failure for {}, rethrowing", repairResource, e);
                throw e;
            }
        }
    }

    private Collection<LockFactory.DistributedLock> getRepairResourceLocks(
                                                                           final LockFactory lockFactory,
                                                                           final Collection<RepairResource> repairResources,
                                                                           final Map<String, String> metadata,
                                                                           final int priority,
                                                                           final UUID nodeId)
                                                                                               throws LockException
    {
        try (TemporaryLockHolder lockHolder = new TemporaryLockHolder())
        {
            for (RepairResource repairResource : repairResources)
            {
                try
                {
                    lockHolder.add(getLockForRepairResource(lockFactory, repairResource, metadata, priority, nodeId));
                }
                catch (LockException e)
                {
                    LOG.debug("{} - Unable to get repair resource lock '{}', releasing previously acquired locks",
                            this,
                            repairResource,
                            e);
                    throw e;
                }
            }

            return lockHolder.getAndClear();
        }
    }

    private LockFactory.DistributedLock getLockForRepairResource(
                                                                 final LockFactory lockFactory,
                                                                 final RepairResource repairResource,
                                                                 final Map<String, String> metadata,
                                                                 final int priority,
                                                                 final UUID nodeId)
                                                                                     throws LockException
    {
        LockFactory.DistributedLock myLock;
        String dataCenter = repairResource.getDataCenter();
        int locksPerResource = getLocksPerResource();

        int startLock = myLockCounter.get();
        for (int i = 0; i < locksPerResource; i++)
        {
            int lockNumber = ((startLock + i) % locksPerResource) + 1;
            String resource = repairResource.getResourceName(lockNumber);
            try
            {
                myLock = lockFactory.tryLock(dataCenter, resource, priority, metadata, nodeId);

                if (myLock != null)
                {
                    myLockCounter.set((lockNumber) % locksPerResource);
                    return myLock;
                }
            }
            catch (LockException e)
            {
                LOG.debug("Lock ({} in datacenter {}) got error, trying next lock",
                        resource,
                        dataCenter,
                        e);
            }
        }

        String msg = String.format("Lock resources exhausted for %s", repairResource);
        LOG.warn(msg);
        throw new LockException(msg);
    }

    static class TemporaryLockHolder implements AutoCloseable
    {
        private final List<LockFactory.DistributedLock> temporaryLocks = new ArrayList<>();

        void add(final LockFactory.DistributedLock lock)
        {
            temporaryLocks.add(lock);
        }

        Collection<LockFactory.DistributedLock> getAndClear()
        {
            Collection<LockFactory.DistributedLock> allLocks = new ArrayList<>(temporaryLocks);
            temporaryLocks.clear();
            return allLocks;
        }

        @Override
        public void close()
        {
            for (LockFactory.DistributedLock lock : temporaryLocks)
            {
                try
                {
                    lock.close();
                }
                catch (Exception e)
                {
                    LOG.warn("Unable to release temporary lock {} for {} ", lock, this, e);
                }
            }
        }
    }
}
