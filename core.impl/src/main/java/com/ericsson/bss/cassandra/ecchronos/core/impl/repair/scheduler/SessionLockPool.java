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
 * Manages distributed locks during a repair session.
 * <p>
 * The pool supports two modes, selected at construction time:
 * <ul>
 *   <li><strong>Batched</strong> ({@code batched = true}, the default session behaviour): locks are acquired
 *       per-task via {@link RepairLockFactoryImpl} and held for the duration of the session. If a task shares
 *       resources with a previously executed task, those locks are reused. All locks are released when the session
 *       ends via {@link #close()}. This minimises lock-acquisition overhead but holds the acquired replica-set
 *       locks for the whole session window.</li>
 *   <li><strong>Non-batched</strong> ({@code batched = false}, "sidecar" behaviour): locks are acquired for a task
 *       and released as soon as that task has been processed via {@link #releaseTaskLocks()}. Nothing is reused or
 *       held across tasks, so a replica-set lock is held only for the duration of a single repair task. This
 *       mirrors the per-task lock scope of the sidecar architecture and avoids monopolising replica-set locks for
 *       the whole session window.</li>
 * </ul>
 * If a lock cannot be acquired for a task, the task is skipped (not the entire job).
 */
final class SessionLockPool implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(SessionLockPool.class);

    private final LockFactory myLockFactory;
    private final RepairLockFactoryImpl myRepairLockFactory;
    private final UUID myNodeId;
    private final boolean myBatched;
    private final Set<RepairResource> myHeldResources = new HashSet<>();
    private final List<LockFactory.DistributedLock> myHeldLocks = new ArrayList<>();
    // Locks acquired for the task currently being processed. In non-batched mode these are released by
    // releaseTaskLocks() as soon as the task completes; in batched mode they are promoted into myHeldLocks.
    private final List<LockFactory.DistributedLock> myCurrentTaskLocks = new ArrayList<>();

    /**
     * Create a pool in batched mode (locks held for the whole session).
     *
     * @param lockFactory the lock factory.
     * @param repairLockFactory the repair lock factory.
     * @param nodeId the node identifier the session runs for.
     */
    SessionLockPool(final LockFactory lockFactory, final RepairLockFactoryImpl repairLockFactory, final UUID nodeId)
    {
        this(lockFactory, repairLockFactory, nodeId, true);
    }

    /**
     * Create a pool in the requested mode.
     *
     * @param lockFactory the lock factory.
     * @param repairLockFactory the repair lock factory.
     * @param nodeId the node identifier the session runs for.
     * @param batched {@code true} to hold locks for the whole session and reuse shared resources across tasks;
     *                {@code false} to release each task's locks as soon as the task completes (sidecar semantics).
     */
    SessionLockPool(final LockFactory lockFactory, final RepairLockFactoryImpl repairLockFactory, final UUID nodeId,
            final boolean batched)
    {
        myLockFactory = lockFactory;
        myRepairLockFactory = repairLockFactory;
        myNodeId = nodeId;
        myBatched = batched;
    }

    /**
     * Try to ensure locks are held for all resources required by the given task.
     * <p>
     * In batched mode, resources already held by the session are reused and only missing resources trigger lock
     * acquisition. In non-batched mode, each task acquires locks for all of its resources (nothing is retained
     * across tasks) and the caller must invoke {@link #releaseTaskLocks()} once the task has been processed.
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
        if (myBatched)
        {
            myHeldLocks.add(lock);
            myHeldResources.addAll(missing);
        }
        else
        {
            myCurrentTaskLocks.add(lock);
            myHeldResources.addAll(missing);
        }
        LOG.debug("Node {}: acquired locks for {} resources (batched={}), total held: {}",
                myNodeId, missing.size(), myBatched, myHeldResources.size() + myCurrentTaskLocks.size());
    }

    /**
     * Release the locks acquired for the task currently being processed.
     * <p>
     * In batched mode this is a no-op (locks are retained until {@link #close()}). In non-batched mode it releases
     * every lock acquired for the current task so the replica-set lock is held only for that single task's
     * duration (sidecar semantics). Should be called after each task is processed.
     */
    void releaseTaskLocks()
    {
        if (myBatched || myCurrentTaskLocks.isEmpty())
        {
            return;
        }
        for (LockFactory.DistributedLock lock : myCurrentTaskLocks)
        {
            try
            {
                lock.close();
            }
            catch (Exception e)
            {
                LOG.warn("Failed to release per-task lock", e);
            }
        }
        myCurrentTaskLocks.clear();
        myHeldResources.clear();
    }

    /**
     * Release all held locks. Called at end of session. Also releases any locks still held for the current task
     * (defensive; non-batched callers should have released them via {@link #releaseTaskLocks()}).
     */
    @Override
    public void close()
    {
        releaseTaskLocks();
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
