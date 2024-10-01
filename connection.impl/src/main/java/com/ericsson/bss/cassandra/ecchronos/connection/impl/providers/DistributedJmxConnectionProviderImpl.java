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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.providers;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.remote.JMXConnector;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder;

import com.datastax.oss.driver.api.core.metadata.Node;

public class DistributedJmxConnectionProviderImpl implements DistributedJmxConnectionProvider
{
    private final List<Node> myNodesList;
    private final ConcurrentHashMap<UUID, JMXConnector> myJMXConnections;

    /**
     * Constructs a DistributedJmxConnectionProviderImpl with the specified list of nodes and JMX connections.
     *
     * @param nodesList
     *         the list of Node objects representing the nodes to manage JMX connections for.
     * @param jmxConnections
     *         a ConcurrentHashMap mapping each node's UUID to its corresponding JMXConnector.
     */
    public DistributedJmxConnectionProviderImpl(
            final List<Node> nodesList,
            final ConcurrentHashMap<UUID, JMXConnector> jmxConnections
    )
    {
        myNodesList = nodesList;
        myJMXConnections = jmxConnections;
    }

    /**
     * validate if the given JMXConnector is available.
     *
     * @param jmxConnector
     *            The jmxConnector to validate
     * @return A boolean representing the node's connection status.
     */
    @Override
    public boolean isConnected(final JMXConnector jmxConnector)
    {
        try
        {
            jmxConnector.getConnectionId();
        }
        catch (IOException e)
        {
            return false;
        }

        return true;
    }

    /**
     * Checks if the JMX connection for the specified node is active.
     *
     * @param nodeID
     *         the UUID of the node to check the connection status for.
     * @return true if the JMX connection for the specified node is active, false otherwise.
     */
    public boolean isConnected(final UUID nodeID)
    {
        return isConnected(myJMXConnections.get(nodeID));
    }

    /**
     * Creates and returns a new instance of the DistributedJmxBuilder.
     *
     * @return a new DistributedJmxBuilder instance.
     */
    public static DistributedJmxBuilder builder()
    {
        return new DistributedJmxBuilder();
    }

    /**
     * Get the map of JMX connections.
     *
     * @return a ConcurrentHashMap where the key is the UUID of a node and the value is the corresponding JMXConnector.
     */
    @Override
    public ConcurrentHashMap<UUID, JMXConnector> getJmxConnections()
    {
        return myJMXConnections;
    }

    /**
     * Get the JMXConnector for a specific node.
     *
     * @param nodeID
     *         the UUID of the node for which to retrieve the JMXConnector.
     * @return the JMXConnector associated with the specified nodeID, or null if no such connection exists.
     */
    @Override
    public JMXConnector getJmxConnector(final UUID nodeID)
    {
        return myJMXConnections.get(nodeID);
    }

    /**
     * Close all JMX connections.
     *
     * @throws IOException
     *         if an I/O error occurs during the closing of connections.
     */
    @Override
    public void close() throws IOException
    {
        for (int i = 0; i <= myNodesList.size(); i++)
        {
            close(myNodesList.get(i).getHostId());
        }
    }

    /**
     * Close the JMX connection for a specific node.
     *
     * @param nodeID
     *         the UUID of the node whose JMX connection should be closed.
     * @throws IOException
     *         if an I/O error occurs while closing the connection.
     */
    @Override
    public void close(final UUID nodeID) throws IOException
    {
        myJMXConnections.get(nodeID).close();
    }

}
