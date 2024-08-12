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
package com.ericsson.bss.cassandra.ecchronos.data.exceptions;

/**
 * Generic exception thrown by schedulers to signal that something went wrong.
 */
public class EcChronosException extends Exception
{
    private static final long serialVersionUID = 1148561336907867613L;

    public EcChronosException(final String message)
    {
        super(message);
    }

    public EcChronosException(final Throwable t)
    {
        super(t);
    }

    public EcChronosException(final String message, final Throwable t)
    {
        super(message, t);
    }
}
