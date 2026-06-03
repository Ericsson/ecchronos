/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Manages distributed locks during a repair session using incremental accumulation.
 * <p>
 * Locks are acquired per-task and held for the duration of the session.
 * If a task shares resources with a previously executed task, those locks are reused.
 * If a lock cannot be acquired for a task, the task is skipped (not the entire job).
 * All locks are released when the session ends via {@link #close()}.
 */
final class SessionLockPool implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(SessionLockPool.class);

    private final LockFactory myLockFactory;
    private final UUID myNodeId;
    private final Map<RepairResource, LockFactory.DistributedLock> myHeldLocks = new HashMap<>();

    SessionLockPool(final LockFactory lockFactory, final UUID nodeId)
    {
        myLockFactory = lockFactory;
        myNodeId = nodeId;
    }

    /**
     * Try to ensure locks are held for all resources required by the given task.
     * Resources already held are reused. Only missing resources trigger CAS operations.
     * If any new resource cannot be acquired, previously acquired new locks from this
     * call are released and a LockException is thrown.
     *
     * @param task The task whose resources need to be locked.
     * @throws LockException If unable to acquire a required lock.
     */
    void acquireForTask(final ScheduledTask task) throws LockException
    {
        Set<RepairResource> required = task.getRepairResources();
        if (required.isEmpty())
        {
            return;
        }

        Set<RepairResource> missing = new HashSet<>();
        for (RepairResource resource : required)
        {
            if (!myHeldLocks.containsKey(resource))
            {
                missing.add(resource);
            }
        }

        if (missing.isEmpty())
        {
            return;
        }

        Map<RepairResource, LockFactory.DistributedLock> newlyAcquired = new HashMap<>();
        for (RepairResource resource : missing)
        {
            try
            {
                LockFactory.DistributedLock lock = myLockFactory.tryLock(
                        resource.getDataCenter(),
                        resource.getResourceName(1),
                        task.getPriority(),
                        new HashMap<>(),
                        myNodeId);
                newlyAcquired.put(resource, lock);
            }
            catch (LockException e)
            {
                // Keep successfully acquired locks for future tasks
                myHeldLocks.putAll(newlyAcquired);
                LOG.debug("Node {}: failed to acquire lock for {}, kept {} new locks",
                        myNodeId, resource, newlyAcquired.size(), e);
                throw e;
            }
        }
        myHeldLocks.putAll(newlyAcquired);
        LOG.debug("Node {}: acquired {} new locks, total held: {}",
                myNodeId, newlyAcquired.size(), myHeldLocks.size());
    }

    /**
     * Release all held locks. Called at end of session.
     */
    @Override
    public void close()
    {
        if (!myHeldLocks.isEmpty())
        {
            LOG.debug("Releasing {} session locks for node {}", myHeldLocks.size(), myNodeId);
            releaseAll(myHeldLocks);
            myHeldLocks.clear();
        }
    }

    private void releaseAll(final Map<RepairResource, LockFactory.DistributedLock> locks)
    {
        for (Map.Entry<RepairResource, LockFactory.DistributedLock> entry : locks.entrySet())
        {
            try
            {
                entry.getValue().close();
            }
            catch (Exception e)
            {
                LOG.warn("Failed to release lock for resource {}", entry.getKey(), e);
            }
        }
    }
}
