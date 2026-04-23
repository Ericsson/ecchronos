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
 * Exception thrown when a lock cannot be acquired due to contention with another node.
 * Unlike regular LockException, this should not be cached since the contention is transient.
 */
public class LockContentionException extends LockException
{
    private static final long serialVersionUID = 2799812379389641955L;

    public LockContentionException(final String message)
    {
        super(message);
    }
}
