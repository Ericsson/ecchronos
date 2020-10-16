/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;

/**
 * Node resolver interface.
 */
public interface NodeResolver
{
    /**
     * Retrieve a node based on public ip address.
     *
     * @param inetAddress The public ip address of the node instance.
     * @return The node.
     */
    Optional<Node> fromIp(InetAddress inetAddress);

    Optional<Node> fromUUID(UUID nodeId);
}
