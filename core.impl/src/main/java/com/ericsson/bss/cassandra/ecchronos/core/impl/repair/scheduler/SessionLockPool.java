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

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairLockFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Manages distributed locks during a repair session using incremental accumulation.
 * <p>
 * Locks are acquired per-task via {@link RepairLockFactoryImpl} and held for the duration of the session.
 * If a task shares resources with a previously executed task, those locks are reused.
 * If a lock cannot be acquired for a task, the task is skipped (not the entire job).
 * All locks are released when the session ends via {@link #close()}.
 */
final class SessionLockPool implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(SessionLockPool.class);

    private final LockFactory myLockFactory;
    private final RepairLockFactoryImpl myRepairLockFactory;
    private final UUID myNodeId;
    private final Set<RepairResource> myHeldResources = new HashSet<>();
    private final List<LockFactory.DistributedLock> myHeldLocks = new ArrayList<>();

    SessionLockPool(final LockFactory lockFactory, final RepairLockFactoryImpl repairLockFactory, final UUID nodeId)
    {
        myLockFactory = lockFactory;
        myRepairLockFactory = repairLockFactory;
        myNodeId = nodeId;
    }

    /**
     * Try to ensure locks are held for all resources required by the given task.
     * Resources already held are reused. Only missing resources trigger lock acquisition.
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
            if (!myHeldResources.contains(resource))
            {
                missing.add(resource);
            }
        }

        if (missing.isEmpty())
        {
            return;
        }

        Map<String, String> metadata = task.getLockMetadata();
        LockFactory.DistributedLock lock = myRepairLockFactory.getLock(
                myLockFactory, missing, metadata, task.getPriority(), myNodeId);
        myHeldLocks.add(lock);
        myHeldResources.addAll(missing);
        LOG.debug("Node {}: acquired locks for {} resources, total held: {}",
                myNodeId, missing.size(), myHeldResources.size());
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
            for (LockFactory.DistributedLock lock : myHeldLocks)
            {
                try
                {
                    lock.close();
                }
                catch (Exception e)
                {
                    LOG.warn("Failed to release session lock", e);
                }
            }
            myHeldLocks.clear();
            myHeldResources.clear();
        }
    }
}
