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
package com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import java.util.UUID;

@SuppressWarnings("VisibilityModifier")
public abstract class ScheduledTask
{
    protected final int myPriority;

    protected ScheduledTask()
    {
        this(1);
    }

    protected ScheduledTask(final int priority)
    {
        myPriority = priority;
    }

    public final boolean preValidate()
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
    public abstract boolean execute(UUID nodeID) throws ScheduledJobException;

    /**
     * Cleanup of the task that should be run after the task has been executed.
     */
    public void cleanup()
    {
        // Let subclasses override
    }
}

