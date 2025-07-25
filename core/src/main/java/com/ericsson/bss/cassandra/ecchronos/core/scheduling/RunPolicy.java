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

@FunctionalInterface
public interface RunPolicy
{
    /**
     * Validate if the job is runnable or how long it should wait until it is tried again.
     *
     * @param job
     *            The job that wants to execute.
     * @return The time until the job should be tried again in milliseconds or -1 if the job can run now.
     */
    long validate(ScheduledJob job);
}
