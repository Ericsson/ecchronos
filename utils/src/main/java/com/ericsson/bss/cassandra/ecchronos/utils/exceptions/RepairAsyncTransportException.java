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
 * An <em>unchecked</em> exception indicating that the JMX {@code StorageService.repairAsync} call failed on the
 * response path, so no repair command id was obtained — but the repair may already have started on Cassandra. This
 * is a transport-layer glitch (for example a Jolokia {@code HTTP error 100 (Continue)}, a connection reset, or a
 * timeout completing the exec), not a repair failure.
 * <p>
 * It is deliberately unchecked so the {@code repairAsync} proxy signature is unchanged: a raw Jolokia
 * {@link RuntimeException} already escapes the proxy today, and this type simply makes that case recognisable to
 * {@code RepairTask}, which decides whether to adopt an already-started session (incremental, discoverable via
 * {@code getRepairSessions}) or to classify the task as transient and retryable (vnode, or when no session is
 * found). See issue #1892.
 */
public class RepairAsyncTransportException extends RuntimeException
{
    private static final long serialVersionUID = 7638482952374051246L;

    /**
     * Constructs a new RepairAsyncTransportException with the specified detail message.
     *
     * @param message the detail message.
     */
    public RepairAsyncTransportException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a new RepairAsyncTransportException with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param cause the cause of the exception.
     */
    public RepairAsyncTransportException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
