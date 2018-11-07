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

public interface ScheduleManager
{
    /**
     * Schedule the provided job for running.
     *
     * @param job
     *            The job to schedule.
     */
    void schedule(ScheduledJob job);

    /**
     * Remove the provided job from the scheduling.
     *
     * @param job
     *            The job to deschedule.
     */
    void deschedule(ScheduledJob job);
}
