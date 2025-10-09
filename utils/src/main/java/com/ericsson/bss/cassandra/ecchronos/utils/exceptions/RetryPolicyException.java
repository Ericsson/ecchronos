/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
 * An exception indicating that the connection with Cassandra
 * Was not possible, which can means that the Cassandra is
 * overloaded or wrong connection configuration.
 */
public class RetryPolicyException extends RuntimeException
{
    private static final long serialVersionUID = 8519513326549621415L;

    public RetryPolicyException(final String message)
    {
        super(message);
    }
}
