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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy.ConnectionResult;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.ConnectionUtils;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DistributedJmxBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedJmxBuilder.class);

    private static final int MAX_PARALLEL_CONNECTIONS = 10;
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ConcurrentHashMap<UUID, JMXConnector> myJMXConnections = new ConcurrentHashMap<>();
    private EccNodesSync myEccNodesSync;
    private JmxConnectionStrategy jmxConnectionStrategy;

    /**
     * Set the map of nodes to be used by the DistributedJmxBuilder.
     *
     * @param nativeConnection
     *         connection bean that contains the Cassandra nodes to connect to.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withNativeConnection(final DistributedNativeConnectionProvider nativeConnection)
    {
        myNativeConnectionProvider = nativeConnection;
        return this;
    }

    /**
     * Set the EccNodesSync instance to be used by the DistributedJmxBuilder.
     *
     * @param eccNodesSync
     *         the EccNodesSync instance that handles synchronization of ECC nodes.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withEccNodesSync(final EccNodesSync eccNodesSync)
    {
        myEccNodesSync = eccNodesSync;
        return this;
    }

    public final DistributedJmxBuilder withConnectionStrategy(final JmxConnectionStrategy strategy)
    {
        jmxConnectionStrategy = strategy;
        return this;
    }

    /**
     * Build the DistributedJmxConnectionProviderImpl instance.
     *
     * @return a new instance of DistributedJmxConnectionProviderImpl initialized with the current settings.
     * @throws IOException
     *         if an I/O error occurs during the creation of connections.
     */
    public final DistributedJmxConnectionProvider build() throws IOException
    {
        createConnections();
        return new DistributedJmxConnectionProviderImpl(this);
    }

    private void createConnections() throws IOException
    {
        Map<UUID, Node> nodes = myNativeConnectionProvider.getNodes();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(nodes.size(), MAX_PARALLEL_CONNECTIONS));
        try
        {
            for (Node node : nodes.values())
            {
                pool.submit(() ->
                {
                    LOG.info("Creating connection with node {}", node.getHostId());
                    try
                    {
                        reconnect(node);
                        LOG.info("Connection created with success");
                    }
                    catch (EcChronosException e)
                    {
                        LOG.info("Unable to connect with node {} connection refused", node.getHostId(), e);
                    }
                });
            }
        }
        finally
        {
            pool.shutdown();
            try
            {
                if (!pool.awaitTermination(ConnectionUtils.JMX_CONNECTION_TIMEOUT + MAX_PARALLEL_CONNECTIONS, TimeUnit.SECONDS))
                {
                    pool.shutdownNow();
                }
            }
            catch (InterruptedException e)
            {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /***
     * Creates a JMX connection to the host.
     * @param node the node to connect with.
     */
    public void reconnect(final Node node) throws EcChronosException
    {
        try
        {
            ConnectionResult result = jmxConnectionStrategy.connect(node);
            verifyConnection(node, result.connector(), result.serviceURL());
        }
        catch (IOException | SecurityException e)
        {
            LOG.error("Failed to create JMX connection with node {} because of", node.getHostId(), e);
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
        }
    }

    private void verifyConnection(final Node node, final JMXConnector jmxConnector, final JMXServiceURL jmxUrl)
    {
        if (ConnectionUtils.isConnected(jmxConnector))
        {
            LOG.info("Connected JMX for {}", jmxUrl);
            myEccNodesSync.updateNodeStatus(NodeStatus.AVAILABLE, node.getDatacenter(), node.getHostId());
            JMXConnector oldConnector = myJMXConnections.put(Objects.requireNonNull(node.getHostId()), jmxConnector);
            ConnectionUtils.closeQuietly(oldConnector);
        }
        else
        {
            ConnectionUtils.closeQuietly(jmxConnector);
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
        }
    }

    public final DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    public final ConcurrentHashMap<UUID, JMXConnector> getJMXConnections()
    {
        return myJMXConnections;
    }
}
