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
 * A {@link LockException} indicating that a lock could not be acquired because of a <em>client-side</em> timeout
 * (for example a CQL driver request timeout), rather than because the Cassandra cluster was unavailable.
 * <p>
 * This typically means ecChronos itself is saturated — its request or CPU budget cannot keep up — so the failure
 * should <strong>not</strong> be attributed to Cassandra being down. Callers can treat this distinctly, for example
 * by applying an escalating backoff so they stop hammering the lock resource at full rate while saturated.
 */
public class LockClientSaturationException extends LockException
{
    private static final long serialVersionUID = 7845219283746501923L;

    /**
     * Constructs a new LockClientSaturationException with the specified detail message.
     *
     * @param message the detail message.
     */
    public LockClientSaturationException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a new LockClientSaturationException with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param t the cause of the exception.
     */
    public LockClientSaturationException(final String message, final Throwable t)
    {
        super(message, t);
    }
}
