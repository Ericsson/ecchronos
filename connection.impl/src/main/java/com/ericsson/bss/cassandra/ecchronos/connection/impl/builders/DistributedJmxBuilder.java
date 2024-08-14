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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.data.enums.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.data.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DistributedJmxBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedJmxBuilder.class);
    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final int DEFAULT_PORT = 7199;

    private CqlSession mySession;
    private List<Node> myNodesList;
    private final ConcurrentHashMap<UUID, JMXConnector> myJMXConnections = new ConcurrentHashMap<>();
    private Supplier<String[]> myCredentialsSupplier;
    private Supplier<Map<String, String>> myTLSSupplier;

    private EccNodesSync myEccNodesSync;

    /**
     * Set the CQL session to be used by the DistributedJmxBuilder.
     *
     * @param session
     *         the CqlSession instance to be used for communication with Cassandra.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withCqlSession(final CqlSession session)
    {
        mySession = session;
        return this;
    }

    /**
     * Set the list of nodes to be used by the DistributedJmxBuilder.
     *
     * @param nodesList
     *         a List of Node instances representing the Cassandra nodes to connect to.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withNodesList(final List<Node> nodesList)
    {
        myNodesList = nodesList;
        return this;
    }

    /**
     * Set the credentials supplier to be used by the DistributedJmxBuilder.
     *
     * @param credentials
     *         a Supplier that provides an array of Strings containing the username and password.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withCredentials(final Supplier<String[]> credentials)
    {
        myCredentialsSupplier = credentials;
        return this;
    }

    /**
     * Set the TLS settings supplier to be used by the DistributedJmxBuilder.
     *
     * @param tlsSupplier
     *         a Supplier that provides a Map containing TLS settings.
     * @return the current instance of DistributedJmxBuilder for chaining.
     */
    public final DistributedJmxBuilder withTLS(final Supplier<Map<String, String>> tlsSupplier)
    {
        myTLSSupplier = tlsSupplier;
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

    /**
     * Build the DistributedJmxConnectionProviderImpl instance.
     *
     * @return a new instance of DistributedJmxConnectionProviderImpl initialized with the current settings.
     * @throws IOException
     *         if an I/O error occurs during the creation of connections.
     */
    public final DistributedJmxConnectionProviderImpl build() throws IOException
    {
        createConnections();
        return new DistributedJmxConnectionProviderImpl(
                myNodesList,
                myJMXConnections
        );
    }

    private void createConnections() throws IOException
    {
        for (Node node : myNodesList)
        {
            LOG.info("Creating connection with node {}", node.getHostId());
            try
            {
                reconnect(node);
                LOG.info("Connection created with success");
            }
            catch (EcChronosException e)
            {
                LOG.info("Unable to connect with node {} connection refused: {}", node.getHostId(), e.getMessage());
            }
        }
    }

    private void reconnect(final Node node) throws IOException, EcChronosException
    {
        try
        {
            String host = node.getBroadcastRpcAddress().get().getHostString();
            Integer port = getJMXPort(node);
            if (host.contains(":"))
            {
                // Use square brackets to surround IPv6 addresses
                host = "[" + host + "]";
            }

            LOG.info("Starting to instantiate JMXService with host: {} and port: {}", host, port);
            JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_FORMAT_URL, host, port));
            LOG.debug("Connecting JMX through {}, credentials: {}, tls: {}", jmxUrl, isAuthEnabled(), isTLSEnabled());
            JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, createJMXEnv());
            if (isConnected(jmxConnector))
            {
                LOG.info("Connected JMX for {}", jmxUrl);
                myEccNodesSync.updateNodeStatus(NodeStatus.AVAILABLE, node.getDatacenter(), node.getHostId());
                myJMXConnections.put(Objects.requireNonNull(node.getHostId()), jmxConnector);
            }
            else
            {
                myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
        }
        }
        catch
        (
            AllNodesFailedException|QueryExecutionException|IOException|SecurityException e)
        {
            LOG.error("Failed to create JMX connection with node {} because of {}", node.getHostId(), e.getMessage());
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
        }
    }

    private Map<String, Object> createJMXEnv()
    {
        Map<String, Object> env = new HashMap<>();
        String[] credentials = getCredentialsConfig();
        Map<String, String> tls = getTLSConfig();
        if (credentials != null)
        {
            env.put(JMXConnector.CREDENTIALS, credentials);
        }

        if (!tls.isEmpty())
        {
            for (Map.Entry<String, String> configEntry : tls.entrySet())
            {
                String key = configEntry.getKey();
                String value = configEntry.getValue();

                if (!value.isEmpty())
                {
                    System.setProperty(key, value);
                }
                else
                {
                    System.clearProperty(key);
                }
            }
            env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
        }
        return env;
    }

    private String[] getCredentialsConfig()
    {
        return myCredentialsSupplier.get();
    }

    private Map<String, String> getTLSConfig()
    {
        return myTLSSupplier.get();
    }

    private boolean isAuthEnabled()
    {
        return getCredentialsConfig() != null;
    }

    private boolean isTLSEnabled()
    {
        return !getTLSConfig().isEmpty();
    }

    private Integer getJMXPort(final Node node)
    {
        SimpleStatement simpleStatement = SimpleStatement
                .builder("SELECT value FROM system_views.system_properties WHERE name = 'cassandra.jmx.remote.port';")
                .setNode(node)
                .build();
        Row row = mySession.execute(simpleStatement).one();
        if (row != null)
        {
            return Integer.parseInt(Objects.requireNonNull(row.getString("value")));
        }
        else
        {
            return DEFAULT_PORT;
        }
    }

    private static boolean isConnected(final JMXConnector jmxConnector)
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
}
