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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.net.InetAddress;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;

/**
 * Interface used to determine node statuses.
 */
public interface HostStates
{
    /**
     * Check if a host is up.
     *
     * @param address
     *            The broadcast address of the host.
     * @return True if the node is up. False will be returned if the state is unknown or if the host is down.
     */
    boolean isUp(InetAddress address);

    /**
     * Check if a host is up.
     *
     * @param host
     *            The host.
     * @return True if the host is up. False will be returned if the state is unknown or if the host is down.
     */
    boolean isUp(Host host);

    /**
     * Check if a node is up.
     *
     * @param node The node.
     * @return True if the node is up. False will be returned if the state is unknown or if the node is down.
     */
    boolean isUp(Node node);
}
