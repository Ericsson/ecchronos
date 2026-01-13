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
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import org.jolokia.client.jmxadapter.JolokiaJmxConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class DistributedJmxBuilder //NOPMD Possible God Class
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedJmxBuilder.class);
    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final String JMX_JOLOKIA_FORMAT_URL = "service:jmx:jolokia://%s:%d/jolokia/";
    private static final Integer JMX_JOLOKIA_CONNECTION_TIMEOUT = 20;
    private static final int DEFAULT_JOLOKIA_PORT = 8778;
    private static final int DEFAULT_PORT = 7199;
    public static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP

    private CqlSession mySession;
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ConcurrentHashMap<UUID, JMXConnector> myJMXConnections = new ConcurrentHashMap<>();
    private Supplier<String[]> myCredentialsSupplier;
    private Supplier<Map<String, String>> myTLSSupplier;
    private CertificateHandler myCertificateHandler;
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

    public final DistributedJmxBuilder withCertificateHandler(final CertificateHandler certificateHandler)
    {
        myCertificateHandler = certificateHandler;
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
        return new DistributedJmxConnectionProviderImpl(
                this
        );
    }

    private void createConnections() throws IOException
    {
        for (Node node : myNativeConnectionProvider.getNodes().values())
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

    /***
     * Creates a JMX connection to the host.
     * @param node the node to connect with.
     */
    public void reconnect(final Node node) throws EcChronosException //NOPMD CyclomaticComplexity
    {
        try
        {
            String host = node.getBroadcastRpcAddress().get().getAddress().getHostAddress();
            if (NO_BROADCAST_ADDRESS.equals(host))
            {
                host = node.getListenAddress().get().getHostString();
            }
            if (myIpTranslator.isActive())
            {
                host = myIpTranslator.getInternalIp(host);
            }
            JMXServiceURL jmxUrl;
            Integer port;
            JMXConnector jmxConnector;

            if (myReverseDNSResolution)
            {
                host = ReverseDNS.fromHostString(host);
            }
            if (host.contains(":") && !host.startsWith("[") && !host.endsWith("]"))
            {
                // Use square brackets to surround IPv6 addresses
                host = "[" + host + "]";
            }

            if (isJolokiaEnabled)
            {
                port = myJolokiaPort;
                jmxUrl = new JMXServiceURL(String.format(JMX_JOLOKIA_FORMAT_URL, host, port));
                JolokiaJmxConnectionProvider jolokiaJmxConnectionProvider = new JolokiaJmxConnectionProvider();
                LOG.info("Creating Jolokia JMXConnection with host: {} and port: {}", host, port);
                jmxConnector = jolokiaJmxConnectionProvider.newJMXConnector(jmxUrl, createJMXEnv());

                ExecutorService exec = Executors.newSingleThreadExecutor();
                Future future = exec.submit(() ->
                {
                    try
                    {
                        jmxConnector.connect();
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                try
                {
                    future.get(JMX_JOLOKIA_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
                }
                catch (TimeoutException | InterruptedException | ExecutionException e)
                {
                    future.cancel(true);
                    throw new IOException("Jolokia connection failed due to {}", e);
                }
               // Verify MBeanServerConnection is available
                if (jmxConnector.getMBeanServerConnection() == null)
                {
                    throw new IOException("MBeanServerConnection is null after Jolokia connection");
                }
            }
            else
            {
                port = getJMXPort(node);
                jmxUrl = new JMXServiceURL(String.format(JMX_FORMAT_URL, host, port));
                LOG.info("Starting to instantiate JMXService with host: {} and port: {}", host, port);
                jmxConnector = JMXConnectorFactory.connect(jmxUrl, createJMXEnv());
            }

            LOG.debug("Connecting JMX through {}, credentials: {}, tls: {}", jmxUrl, isAuthEnabled(), isTLSEnabled());
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
                AllNodesFailedException | QueryExecutionException | IOException | SecurityException e)
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

        if (isJolokiaEnabled && myCertificateHandler != null)
        {
            env.put("jmx.remote.x.check.stub", "true");
            myCertificateHandler.setDefaultSSLContext();
        }
        else if (!tls.isEmpty())
        {
            for (Map.Entry<String, String> configEntry : tls.entrySet())
            {
                String key = configEntry.getKey();
                String value = configEntry.getValue();

                if (value != null && !value.isEmpty())
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
        if (myCredentialsSupplier == null)
        {
            return null;
        }
        return myCredentialsSupplier.get();
    }

    private Map<String, String> getTLSConfig()
    {
        if (myTLSSupplier == null)
        {
            return new HashMap<>();
        }
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
        try
        {
            SimpleStatement simpleStatement = SimpleStatement
                    .builder("SELECT value FROM system_views.system_properties WHERE name = 'cassandra.jmx.remote.port';")
                    .setNode(node)
                    .build();
            Row row = mySession.execute(simpleStatement).one();
            if ((row == null) || (row.getString("value") == null))
            {
                simpleStatement = SimpleStatement
                        .builder("SELECT value FROM system_views.system_properties WHERE name = 'cassandra.jmx.local.port';")
                        .setNode(node)
                        .build();
                row = mySession.execute(simpleStatement).one();

            }
            if ((row != null) && (row.getString("value") != null))
            {
                return Integer.parseInt(Objects.requireNonNull(row.getString("value")));
            }
            else
            {
                return DEFAULT_PORT;
            }
        }
        catch (AllNodesFailedException e)
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
    public final DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    public final ConcurrentHashMap<UUID, JMXConnector> getJMXConnections()
    {
        return myJMXConnections;
    }
}
