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
package com.ericsson.bss.cassandra.ecchronos.connection;

import com.datastax.oss.driver.api.core.metadata.Node;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.remote.JMXConnector;

/**
 * Provides distributed JMX connections to Cassandra nodes.
 */
public interface DistributedJmxConnectionProvider extends Closeable
{
    /**
     * Returns the map of JMX connections keyed by node UUID.
     *
     * @return a {@link ConcurrentHashMap} of node UUIDs to {@link JMXConnector} instances.
     */
    ConcurrentHashMap<UUID, JMXConnector> getJmxConnections();

    /**
     * Returns the JMX connector for a specific node, reconnecting if necessary.
     *
     * @param nodeID the UUID of the target node.
     * @return the {@link JMXConnector} for the specified node, or {@code null} if unavailable.
     */
    JMXConnector getJmxConnector(UUID nodeID);

    /**
     * Checks whether the given JMX connector is currently connected.
     *
     * @param jmxConnector the JMX connector to check.
     * @return {@code true} if connected, {@code false} otherwise.
     */
    boolean isConnected(JMXConnector jmxConnector);

    /**
     * Closes this connection provider, releasing all resources.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    default void close() throws IOException
    {
    }

    /**
     * Closes the JMX connection for a specific node.
     *
     * @param nodeID the UUID of the node whose connection should be closed.
     * @throws IOException if an I/O error occurs.
     */
    void close(UUID nodeID) throws IOException;

    /**
     * Adds a node and establishes a JMX connection to it.
     *
     * @param node the node to add.
     * @throws IOException if a connection error occurs.
     */
    void add(Node node) throws IOException;
}
