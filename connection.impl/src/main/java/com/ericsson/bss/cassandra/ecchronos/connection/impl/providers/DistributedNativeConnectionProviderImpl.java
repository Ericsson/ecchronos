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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedNativeBuilder;

import java.io.IOException;
import java.util.List;

public class DistributedNativeConnectionProviderImpl implements DistributedNativeConnectionProvider
{
    private final CqlSession mySession;
    private final List<Node> myNodes;
    private final DistributedNativeBuilder myDistributedNativeBuilder;

    /**
     * Constructs a new {@code DistributedNativeConnectionProviderImpl} with the specified {@link CqlSession} and list
     * of {@link Node} instances.
     *
     * @param session
     *         the {@link CqlSession} used for communication with the Cassandra cluster.
     * @param nodesList
     *         the list of {@link Node} instances representing the nodes in the cluster.
     */
    public DistributedNativeConnectionProviderImpl(
            final CqlSession session,
            final List<Node> nodesList,
            final DistributedNativeBuilder distributedNativeBuilder
    )
    {
        mySession = session;
        myNodes = nodesList;
        myDistributedNativeBuilder = distributedNativeBuilder;
    }

    /**
     * Returns the {@link CqlSession} associated with this connection provider.
     *
     * @return the {@link CqlSession} used for communication with the Cassandra cluster.
     */
    @Override
    public CqlSession getCqlSession()
    {
        return mySession;
    }

    /**
     * Returns the list of {@link Node} instances generated based on the agent connection type.
     *
     * @return a {@link List} of {@link Node} instances representing the nodes in the cluster.
     */
    @Override
    public List<Node> getNodes()
    {
        return myNodes;
    }



    /**
     * Closes the {@link CqlSession} associated with this connection provider.
     *
     * @throws IOException
     *         if an I/O error occurs while closing the session.
     */
    @Override
    public void close() throws IOException
    {
        mySession.close();
    }

    /**
     * Creates a new instance of {@link DistributedNativeBuilder} for building
     * {@link DistributedNativeConnectionProviderImpl} objects.
     *
     * @return a new {@link DistributedNativeBuilder} instance.
     */
    public static DistributedNativeBuilder builder()
    {
        return new DistributedNativeBuilder();
    }

    /**
     * Add a nw node to the list of nodes.
     * @param node
     */
    @Override
    public void addNode(final Node node)
    {
        myNodes.add(node);
    }

    /**
     * Remove node for the list of nodes.
     * @param node
     */
    @Override
    public void removeNode(final Node node)
    {
        myNodes.remove(node);
    }

    /**
     * Checks the node is on the list of specified dc's/racks/nodes.
     * @param node
     * @return
     */
    @Override
    public Boolean confirmNodeValid(final Node node)
    {
        return myDistributedNativeBuilder.confirmNodeValid(node);
    }

}
