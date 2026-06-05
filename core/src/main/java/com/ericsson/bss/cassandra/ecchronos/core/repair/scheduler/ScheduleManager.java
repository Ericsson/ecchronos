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

import java.util.Collection;
import java.util.UUID;

public interface ScheduleManager
{
    /**
     * Schedule the provided job for running.
     *
     * @param job
     *            The job to schedule.
     */
    void schedule(UUID nodeID, ScheduledJob job);

    /**
     * Remove the provided job from the scheduling.
     *
     * @param job
     *            The job to deschedule.
     */
    void deschedule(UUID nodeID, ScheduledJob job);

    /**
     * Retrieves the current status of the job being managed by this scheduler.
     * <p>
     * It's intended for monitoring and logging purposes, allowing users to query the job's current state
     * without affecting its execution.
     *
     * @return A {@code String} representing the current status of the job.
     */
    String getCurrentJobStatus();

    /**
     * Create a ScheduledFuture for  the nodeID.
     * @param nodeID
     */
    void createScheduleFutureForNode(UUID nodeID);
    /**
     * Create a ScheduledFuture for each of the nodes in the nodeIDList.
     * @param nodeIDList
     */
    void createScheduleFutureForNodeIDList(Collection<UUID> nodeIDList);

    /**
     * Remove the ScheduledFuture and associated resources for the given nodeID.
     * @param nodeID the node to remove
     */
    void removeScheduleFutureForNode(UUID nodeID);

    /**
     * Get the current session window in milliseconds.
     * @return session window in ms
     */
    long getSessionWindowInMs();

    /**
     * Set the session window at runtime.
     * @param sessionWindowInMs new session window in milliseconds, must be greater than 0
     */
    void setSessionWindowInMs(long sessionWindowInMs);

    /**
     * Get the current cooldown in milliseconds.
     * @return cooldown in ms
     */
    long getCooldownInMs();

    /**
     * Set the cooldown at runtime.
     * @param cooldownInMs new cooldown in milliseconds, must be greater than or equal to 0
     */
    void setCooldownInMs(long cooldownInMs);

    /**
     * Get the current locks per resource value.
     * @return locks per resource
     */
    int getLocksPerResource();

    /**
     * Set locks per resource at runtime.
     * @param locksPerResource new value, must be at least 1
     */
    void setLocksPerResource(int locksPerResource);
}
