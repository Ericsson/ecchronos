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
import java.util.UUID;

/**
 * An internal representation of a node.
 */
public interface Node
{
    /**
     * Get the host id of the node.
     *
     * @return The host id of the node.
     */
    UUID getId();

    /**
     * Get the public ip address of the node.
     *
     * @return The public ip address of the node.
     */
    InetAddress getPublicAddress();

    /**
     * Get the data center the node resides in.
     *
     * @return The data center of the node.
     */
    String getDatacenter();
}
