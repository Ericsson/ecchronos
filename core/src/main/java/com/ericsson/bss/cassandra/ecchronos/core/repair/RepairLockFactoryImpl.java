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

import com.ericsson.bss.cassandra.ecchronos.core.LockCollection;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RepairLockFactoryImpl implements RepairLockFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairLockFactoryImpl.class);

    private static final int LOCKS_PER_RESOURCE = 1;

    @Override
    public LockFactory.DistributedLock getLock(LockFactory lockFactory, Set<RepairResource> repairResources, Map<String, String> metadata, int priority) throws LockException
    {
        for (RepairResource repairResource : repairResources)
        {
            if (!lockFactory.sufficientNodesForLocking(repairResource.getDataCenter(), repairResource.getResourceName(LOCKS_PER_RESOURCE)))
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

        validateNoCachedFailures(lockFactory, repairResources);

        Collection<LockFactory.DistributedLock> locks = getRepairResourceLocks(lockFactory, repairResources, metadata, priority);

        return new LockCollection(locks);
    }

    private void validateNoCachedFailures(LockFactory lockFactory, Set<RepairResource> repairResources) throws LockException
    {
        for (RepairResource repairResource : repairResources)
        {
            Optional<LockException> cachedException = lockFactory.getCachedFailure(repairResource.getDataCenter(), repairResource.getResourceName(LOCKS_PER_RESOURCE));
            if (cachedException.isPresent())
            {
                LockException e = cachedException.get();
                LOG.debug("Found cached locking failure for {}, rethrowing", repairResource, e);
                throw cachedException.get();
            }
        }
    }

    private Collection<LockFactory.DistributedLock> getRepairResourceLocks(LockFactory lockFactory, Collection<RepairResource> repairResources, Map<String, String> metadata, int priority) throws LockException
    {
        Collection<LockFactory.DistributedLock> locks = new ArrayList<>();

        for (RepairResource repairResource : repairResources)
        {
            try
            {
                locks.add(getLockForRepairResource(lockFactory, repairResource, metadata, priority));
            }
            catch (LockException e)
            {
                LOG.debug("{} - Unable to get lock for repair resource '{}', releasing previously acquired locks - {}", this, repairResource, e.getMessage());
                releaseLocks(locks);
                throw e;
            }
        }

        return locks;
    }

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
                LOG.warn("Unable to release lock {} for {} ", lock, this, e);
            }
        }
    }

    private LockFactory.DistributedLock getLockForRepairResource(LockFactory lockFactory, RepairResource repairResource, Map<String, String> metadata, int priority) throws LockException
    {
        LockFactory.DistributedLock myLock;

        String dataCenter = repairResource.getDataCenter();

        String resource = repairResource.getResourceName(LOCKS_PER_RESOURCE);
        try
        {
            myLock = lockFactory.tryLock(dataCenter, resource, priority, metadata);

            if (myLock != null)
            {
                return myLock;
            }

            String msg = String.format("Lock resources exhausted for %s", repairResource);
            LOG.warn(msg);
            throw new LockException(msg);
        }
        catch (LockException e)
        {
            LOG.debug("Lock ({} in datacenter {}) got error {}", resource, dataCenter, e.getMessage());
            throw e;
        }
    }
}
