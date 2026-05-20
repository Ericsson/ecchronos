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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class DistributedJmxBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedJmxBuilder.class);
    private static final int DEFAULT_JOLOKIA_PORT = 8778;
    private static final int MAX_PARALLEL_CONNECTIONS = 10;
    private static final int CONNECTION_TIMEOUT_EXTRA_SECONDS = 10;

    public static final String ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY = "ecchronos.jolokia.ssl.enabled";
    public static final String JOLOKIA_CA_CERTIFICATE_PROPERTY = "jolokia.caCertificate";
    public static final String JOLOKIA_CLIENT_CERTIFICATE_PROPERTY = "jolokia.clientCertificate";
    public static final String JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY = "jolokia.clientKey";
    public static final String JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY = "jolokia.clientKeyAlgorithm";
    public static final String JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY = "jdk.internal.httpclient.disableHostnameVerification";

    private CqlSession mySession;
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ConcurrentHashMap<UUID, JMXConnector> myJMXConnections = new ConcurrentHashMap<>();
    private Supplier<String[]> myCredentialsSupplier;
    private Supplier<Map<String, String>> myTLSSupplier;
    private boolean isJolokiaEnabled = false;
    private int myJolokiaPort = DEFAULT_JOLOKIA_PORT;
    private EccNodesSync myEccNodesSync;
    private boolean myReverseDNSResolution = false;
    private IpTranslator myIpTranslator;

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

    public final DistributedJmxBuilder withJolokiaEnabled(final boolean jolokiaEnabled)
    {
        isJolokiaEnabled = jolokiaEnabled;
        return this;
    }

    public final DistributedJmxBuilder withJolokiaPort(final int jolokiaPort)
    {
        myJolokiaPort = jolokiaPort;
        return this;
    }

    public final DistributedJmxBuilder withDNSResolution(final boolean reverseDNSResolution)
    {
        myReverseDNSResolution = reverseDNSResolution;
        return this;
    }

    public final DistributedJmxBuilder withIpTranslator(final IpTranslator ipTranslator)
    {
        myIpTranslator = ipTranslator;
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
                if (!pool.awaitTermination(
                        JolokiaJmxConnectionFactory.CONNECTION_TIMEOUT_SECONDS + CONNECTION_TIMEOUT_EXTRA_SECONDS,
                        TimeUnit.SECONDS))
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
            JmxHostResolver hostResolver = new JmxHostResolver(myIpTranslator, myReverseDNSResolution);
            String host = hostResolver.resolve(node);
            Map<String, Object> env = JmxEnvironmentFactory.create(myCredentialsSupplier, myTLSSupplier, isJolokiaEnabled);
            int port;
            JMXConnector jmxConnector;

            if (isJolokiaEnabled)
            {
                port = myJolokiaPort;
                boolean sslEnabled = String.valueOf(true).equals(
                        myTLSSupplier != null ? myTLSSupplier.get().get(ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY) : null);
                jmxConnector = new JolokiaJmxConnectionFactory(sslEnabled).connect(host, port, env);
            }
            else
            {
                port = JmxPortDiscovery.getPort(mySession, node);
                jmxConnector = new RmiJmxConnectionFactory().connect(host, port, env);
            }

            LOG.debug("Connecting JMX through {}:{}, credentials: {}, tls: {}",
                    host, port, myCredentialsSupplier != null, myTLSSupplier != null);
            if (isConnected(jmxConnector))
            {
                LOG.info("Connected JMX for {}:{}", host, port);
                myEccNodesSync.updateNodeStatus(NodeStatus.AVAILABLE, node.getDatacenter(), node.getHostId());
                JMXConnector oldConnector = myJMXConnections.put(Objects.requireNonNull(node.getHostId()), jmxConnector);
                closeQuietly(oldConnector);
            }
            else
            {
                closeQuietly(jmxConnector);
                myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
            }
        }
        catch (IOException | SecurityException e)
        {
            LOG.error("Failed to create JMX connection with node {} because of", node.getHostId(), e);
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), node.getHostId());
        }
    }

    private static boolean isConnected(final JMXConnector jmxConnector)
    {
        try
        {
            jmxConnector.getConnectionId();
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    private static void closeQuietly(final JMXConnector connector)
    {
        if (connector != null)
        {
            try
            {
                connector.close();
            }
            catch (IOException | NullPointerException e)
            {
                LOG.debug("Failed to close JMX connector", e);
            }
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
