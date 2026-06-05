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
package com.ericsson.bss.cassandra.ecchronos.core.exceptions;

/**
 * Exception thrown when a lock factory is unable to get a lock.
 */
public class LockException extends Exception
{
    private static final long serialVersionUID = 1699712279389641954L;

    /**
     * Constructs a new LockException.
     * @param message the log message
     */
    public LockException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a new LockException.
     * @param message the log message
     * @param t the throwable
     */
    public LockException(final String message, final Throwable t)
    {
        super(message, t);
    }

    /**
     * Constructs a new LockException.
     * @param t the throwable
     */
    public LockException(final Throwable t)
    {
        super(t);
    }
}
