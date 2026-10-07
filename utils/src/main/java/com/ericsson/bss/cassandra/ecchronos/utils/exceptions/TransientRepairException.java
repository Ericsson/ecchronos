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
package com.ericsson.bss.cassandra.ecchronos.utils.exceptions;

/**
 * A {@link ScheduledJobException} indicating a <em>transient</em> repair-task failure that may succeed if retried —
 * for example a JMX connection failure, a stale connection, {@code addStorageServiceListener} returning false, or a
 * repair session failing to start. Contrast with a plain {@link ScheduledJobException}, which (for repair tasks) is
 * treated as a terminal failure such as Cassandra reporting failed ranges or lost notifications. See issue #1848.
 */
public class TransientRepairException extends ScheduledJobException
{
    private static final long serialVersionUID = 4136482952374051245L;

    /**
     * Constructs a new TransientRepairException with the specified detail message.
     *
     * @param message the detail message.
     */
    public TransientRepairException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a new TransientRepairException with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param cause the cause of the exception.
     */
    public TransientRepairException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
