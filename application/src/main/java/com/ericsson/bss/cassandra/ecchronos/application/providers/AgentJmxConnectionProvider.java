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
package com.ericsson.bss.cassandra.ecchronos.application.providers;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.JmxTLSConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Default JmxConnection provider used to create JMX connections.
 */
public class AgentJmxConnectionProvider implements DistributedJmxConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(AgentJmxConnectionProvider.class);
    private final DistributedJmxConnectionProviderImpl myDistributedJmxConnectionProviderImpl;

    /**
     * Constructs an {@code AgentJmxConnectionProvider} with the specified parameters.
     *
     * @param jmxSecurity
     *         a {@link Supplier} providing the JMX security configuration.
     * @param distributedNativeConnectionProvider
     *         the provider responsible for managing native connections in a distributed environment.
     * @param eccNodesSync
     *         an {@link EccNodesSync} instance for synchronizing ECC nodes.
     * @throws IOException
     *         if an I/O error occurs during the initialization of the JMX connection provider.
     */
    public AgentJmxConnectionProvider(
            final Supplier<Security.JmxSecurity> jmxSecurity,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final EccNodesSync eccNodesSync
    ) throws IOException
    {
        Supplier<String[]> credentials = () -> convertCredentials(jmxSecurity);
        Supplier<Map<String, String>> tls = () -> convertTls(jmxSecurity);

        LOG.info("Creating DistributedJmxConnectionConfig");
        myDistributedJmxConnectionProviderImpl = DistributedJmxConnectionProviderImpl.builder()
                .withCqlSession(distributedNativeConnectionProvider.getCqlSession())
                .withNodesList(distributedNativeConnectionProvider.getNodes())
                .withCredentials(credentials)
                .withEccNodesSync(eccNodesSync)
                .withTLS(tls)
                .build();
    }

    private Map<String, String> convertTls(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        JmxTLSConfig tlsConfig = jmxSecurity.get().getJmxTlsConfig();
        if (!tlsConfig.isEnabled())
        {
            return new HashMap<>();
        }

        Map<String, String> config = new HashMap<>();
        if (tlsConfig.getProtocol() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.protocols", tlsConfig.getProtocol());
        }
        if (tlsConfig.getCipherSuites() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.cipher.suites", tlsConfig.getCipherSuites());
        }
        config.put("javax.net.ssl.keyStore", tlsConfig.getKeyStorePath());
        config.put("javax.net.ssl.keyStorePassword", tlsConfig.getKeyStorePassword());
        config.put("javax.net.ssl.trustStore", tlsConfig.getTrustStorePath());
        config.put("javax.net.ssl.trustStorePassword", tlsConfig.getTrustStorePassword());

        return config;
    }

    private String[] convertCredentials(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        Credentials credentials = jmxSecurity.get().getJmxCredentials();
        if (!credentials.isEnabled())
        {
            return null;
        }
        return new String[] {
                credentials.getUsername(), credentials.getPassword()
        };
    }

    /**
     * Retrieves the current map of JMX connections.
     *
     * @return a {@link ConcurrentHashMap} containing the JMX connections, where the key is the UUID of the node and the
     *         value is the {@link JMXConnector}.
     */
    @Override
    public ConcurrentHashMap<UUID, JMXConnector> getJmxConnections()
    {
        return myDistributedJmxConnectionProviderImpl.getJmxConnections();
    }

    /**
     * Retrieves the JMX connector associated with the specified node ID.
     *
     * @param nodeID
     *         the UUID of the node for which to retrieve the JMX connector.
     * @return the {@link JMXConnector} associated with the given node ID.
     */
    @Override
    public JMXConnector getJmxConnector(final UUID nodeID)
    {
        return myDistributedJmxConnectionProviderImpl.getJmxConnector(nodeID);
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
        return myDistributedJmxConnectionProviderImpl.isConnected(jmxConnector);
    }
    /**
     * Closes the JMX connection associated with the specified node ID.
     *
     * @param nodeID
     *         the UUID of the node for which to close the JMX connection.
     * @throws IOException
     *         if an I/O error occurs while closing the connection.
     */
    @Override
    public void close(final UUID nodeID) throws IOException
    {
        myDistributedJmxConnectionProviderImpl.close(nodeID);
    }

    /**
     * Creates a new connection a node.
     * @param node
     *
     * @throws IOException
     */
    @Override
    public void add(final Node node) throws IOException
    {
        myDistributedJmxConnectionProviderImpl.add(node);
    }

    /**
     * Closes all JMX connections managed by this provider.
     *
     * @throws IOException
     *         if an I/O error occurs while closing the connections.
     */
    @Override
    public void close() throws IOException
    {
        myDistributedJmxConnectionProviderImpl.close();
    }
}
