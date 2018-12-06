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
package com.ericsson.bss.cassandra.ecchronos.core.scheduling;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;

import java.util.HashMap;

/**
 * A scheduled task run by the scheduled job.
 */
public abstract class ScheduledTask
{
    private static final String DEFAULT_SCHEDULE_RESOURCE = "SCHEDULE_LOCK";

    protected final int myPriority;

    protected ScheduledTask()
    {
        this(1);
    }

    protected ScheduledTask(int priority)
    {
        myPriority = priority;
    }

    public boolean preValidate()
    {
        return true;
    }

    /**
     * Run the task.
     *
     * @return True if the task was executed successfully.
     * @throws ScheduledJobException
     *             if anything went wrong during running.
     */
    public abstract boolean execute() throws ScheduledJobException;

    /**
     * Cleanup of the task that should be run after the task has been executed.
     */
    public void cleanup()
    {
        // Let subclasses override
    }

    /**
     * Get the lock used by this scheduled job.
     *
     * @param lockFactory
     *            The lock factory to use.
     * @return The lock used by this scheduled job.
     * @throws LockException Thrown when it's not possible to get the lock.
     */
    public LockFactory.DistributedLock getLock(LockFactory lockFactory) throws LockException
    {
        return lockFactory.tryLock(null, DEFAULT_SCHEDULE_RESOURCE, myPriority, new HashMap<>());
    }
}
