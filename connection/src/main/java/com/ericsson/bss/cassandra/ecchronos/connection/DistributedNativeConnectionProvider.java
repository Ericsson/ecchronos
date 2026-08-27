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

import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.io.Closeable;
import java.io.IOException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Map;
import java.util.UUID;

/**
 * Provides distributed native CQL connections to Cassandra nodes.
 */
public interface DistributedNativeConnectionProvider extends Closeable
{
    /**
     * Returns the CQL session used for communication with the Cassandra cluster.
     *
     * @return the {@link CqlSession} instance.
     */
    CqlSession getCqlSession();

    /**
     * Returns the map of managed Cassandra nodes keyed by their host UUID.
     *
     * @return a map of node UUIDs to {@link Node} instances.
     */
    Map<UUID, Node> getNodes();

    /**
     * Closes this connection provider, releasing any resources.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    default void close() throws IOException
    {
    }

    /**
     * Adds a node to the set of managed nodes.
     *
     * @param myNode the node to add.
     */
    void addNode(Node myNode);

    /**
     * Removes a node from the set of managed nodes.
     *
     * @param myNode the node to remove.
     */
    void removeNode(Node myNode);

    /**
     * Confirms whether the given node is valid according to the configured connection type filter.
     *
     * @param node the node to validate.
     * @return {@code true} if the node is valid, {@code false} otherwise.
     */
    Boolean confirmNodeValid(Node node);

    /**
     * Returns the type of connection being used by this provider.
     *
     * @return the {@link ConnectionType}.
     */
    ConnectionType getConnectionType();
}
