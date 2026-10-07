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
package com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler;

/**
 * The outcome of executing a {@link ScheduledTask}.
 * <p>
 * Replaces the previous {@code boolean} success flag so that a job can distinguish a transient failure that is
 * worth retrying (for example a JMX request failure to a node) from a terminal failure that should fail the job
 * immediately (for example Cassandra reporting failed ranges, or a topology change). See issue #1848.
 */
public enum TaskExecutionResult
{
    /** The task completed successfully. */
    SUCCESS,

    /**
     * The task failed for a transient reason (such as a JMX connection problem) and may succeed if retried.
     */
    RETRYABLE,

    /**
     * The task failed for a terminal reason (such as failed ranges reported by Cassandra, a topology change, or
     * lost notifications) and should not be retried.
     */
    TERMINAL;

    /**
     * Whether this result represents a successful execution.
     *
     * @return true if {@link #SUCCESS}.
     */
    public boolean isSuccessful()
    {
        return this == SUCCESS;
    }

    /**
     * Whether this result represents a retryable (transient) failure.
     *
     * @return true if {@link #RETRYABLE}.
     */
    public boolean isRetryable()
    {
        return this == RETRYABLE;
    }
}
