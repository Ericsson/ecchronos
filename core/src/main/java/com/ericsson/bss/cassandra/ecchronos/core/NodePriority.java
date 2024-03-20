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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.util.UUID;

/**
 * Represents a container for node priority configurations and state for the CASLockFactory.
 * This class is used to decouple node priority fields from CASLockFactory to avoid excessive field count.
 */
public final class NodePriority
{
    private final UUID myNode;
    private final int myPriority;

    public NodePriority(final UUID node, final int priority)
    {
        myNode = node;
        myPriority = priority;
    }

    public UUID getUuid()
    {
        return myNode;
    }

    public int getPriority()
    {
        return myPriority;
    }
}
