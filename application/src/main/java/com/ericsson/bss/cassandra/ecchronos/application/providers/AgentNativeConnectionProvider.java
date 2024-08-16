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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.AgentConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.ReloadingAuthProvider;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedNativeBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The {@code AgentNativeConnectionProvider} class is responsible for establishing and managing native connections to
 * Cassandra nodes based on the provided configuration. This class integrates security configurations, such as
 * authentication and TLS, and supports different connection types like datacenter-aware, rack-aware, and host-aware.
 */
public class AgentNativeConnectionProvider implements DistributedNativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(AgentNativeConnectionProvider.class);

    private final DistributedNativeConnectionProviderImpl myDistributedNativeConnectionProviderImpl;

    /**
     * Constructs an {@code AgentNativeConnectionProvider} with the specified configuration, security supplier, and
     * certificate handler.
     *
     * @param config
     *         the configuration object containing the connection settings.
     * @param cqlSecuritySupplier
     *         a {@link Supplier} providing the CQL security settings.
     * @param certificateHandler
     *         the handler for managing SSL/TLS certificates.
     */
    public AgentNativeConnectionProvider(
            final Config config,
            final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
            final CertificateHandler certificateHandler
    )
    {
        AgentConnectionConfig agentConnectionConfig = config.getConnectionConfig().getCqlConnection()
                .getAgentConnectionConfig();
        Security.CqlSecurity cqlSecurity = cqlSecuritySupplier.get();
        boolean authEnabled = cqlSecurity.getCqlCredentials().isEnabled();
        boolean tlsEnabled = cqlSecurity.getCqlTlsConfig().isEnabled();
        AuthProvider authProvider = null;
        if (authEnabled)
        {
            authProvider = new ReloadingAuthProvider(() -> cqlSecuritySupplier.get().getCqlCredentials());
        }

        SslEngineFactory sslEngineFactory = null;
        if (tlsEnabled)
        {
            sslEngineFactory = certificateHandler;
        }

        DistributedNativeBuilder nativeConnectionBuilder =
                DistributedNativeConnectionProviderImpl.builder()
                        .withInitialContactPoints(resolveInitialContactPoints(agentConnectionConfig.getContactPoints()))
                        .withAgentType(agentConnectionConfig.getType().toString())
                        .withLocalDatacenter(agentConnectionConfig.getLocalDatacenter())
                        .withAuthProvider(authProvider)
                        .withSslEngineFactory(sslEngineFactory);
        LOG.info("Preparing Agent Connection Config");
        nativeConnectionBuilder = resolveAgentProviderBuilder(nativeConnectionBuilder, agentConnectionConfig);
        LOG.info("Establishing Connection With Nodes");
        myDistributedNativeConnectionProviderImpl = tryEstablishConnection(nativeConnectionBuilder);
    }

    /**
     * Resolves the connection provider builder based on the specified agent connection configuration. This method
     * configures the builder with the appropriate connection type (datacenter-aware, rack-aware, or host-aware).
     *
     * @param builder
     *         the {@link DistributedNativeBuilder} instance to configure.
     * @param agentConnectionConfig
     *         the connection configuration object.
     * @return the configured {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder resolveAgentProviderBuilder(
            final DistributedNativeBuilder builder,
            final AgentConnectionConfig agentConnectionConfig
    )
    {
        switch (agentConnectionConfig.getType())
        {
        case datacenterAware:
            LOG.info("Using DatacenterAware as Agent Config");
            return builder.withDatacenterAware(resolveDatacenterAware(
                    agentConnectionConfig.getDatacenterAware()));
        case rackAware:
            LOG.info("Using RackAware as Agent Config");
            return builder.withRackAware(resolveRackAware(
                    agentConnectionConfig.getRackAware()));
        case hostAware:
            LOG.info("Using HostAware as Agent Config");
            return builder.withHostAware(resolveHostAware(
                    agentConnectionConfig.getHostAware()));
        default:
        }
        return builder;
    }

    /**
     * Resolves the initial contact points from the provided map of host configurations.
     *
     * @param contactPoints
     *         a map containing the host configurations.
     * @return a list of {@link InetSocketAddress} representing the resolved contact points.
     */
    public final List<InetSocketAddress> resolveInitialContactPoints(
            final Map<String, AgentConnectionConfig.Host> contactPoints
    )
    {
        List<InetSocketAddress> resolvedContactPoints = new ArrayList<>();
        for (AgentConnectionConfig.Host host : contactPoints.values())
        {
            InetSocketAddress tmpAddress = new InetSocketAddress(host.getHost(), host.getPort());
            resolvedContactPoints.add(tmpAddress);
        }
        return resolvedContactPoints;
    }

    /**
     * Resolves the datacenter-aware configuration from the specified {@link AgentConnectionConfig.DatacenterAware}
     * object.
     *
     * @param datacenterAware
     *         the datacenter-aware configuration object.
     * @return a list of datacenter names.
     */
    public final List<String> resolveDatacenterAware(final AgentConnectionConfig.DatacenterAware datacenterAware)
    {
        List<String> datacenterNames = new ArrayList<>();
        for
        (
                AgentConnectionConfig.DatacenterAware.Datacenter datacenter
                :
                datacenterAware.getDatacenters().values())
        {
            datacenterNames.add(datacenter.getName());
        }
        return datacenterNames;
    }

    /**
     * Resolves the rack-aware configuration from the specified {@link AgentConnectionConfig.RackAware} object.
     *
     * @param rackAware
     *         the rack-aware configuration object.
     * @return a list of maps containing datacenter and rack information.
     */
    public final List<Map<String, String>> resolveRackAware(final AgentConnectionConfig.RackAware rackAware)
    {
        List<Map<String, String>> rackList = new ArrayList<>();
        for
        (
                AgentConnectionConfig.RackAware.Rack rack
                :
                rackAware.getRacks().values()
        )
        {
            Map<String, String> rackInfo = new HashMap<>();
            rackInfo.put("datacenterName", rack.getDatacenterName());
            rackInfo.put("rackName", rack.getRackName());
            rackList.add(rackInfo);
        }
        return rackList;
    }

    /**
     * Resolves the host-aware configuration from the specified {@link AgentConnectionConfig.HostAware} object.
     *
     * @param hostAware
     *         the host-aware configuration object.
     * @return a list of {@link InetSocketAddress} representing the resolved hosts.
     */
    public final List<InetSocketAddress> resolveHostAware(final AgentConnectionConfig.HostAware hostAware)
    {
        List<InetSocketAddress> resolvedHosts = new ArrayList<>();
        for
        (
                AgentConnectionConfig.Host host
                :
                hostAware.getHosts().values()
        )
        {
            InetSocketAddress tmpAddress = new InetSocketAddress(host.getHost(), host.getPort());
            resolvedHosts.add(tmpAddress);
        }
        return resolvedHosts;
    }

    /**
     * Attempts to establish a connection to Cassandra nodes using the provided builder. This method handles exceptions
     * and logs errors if the connection fails.
     *
     * @param builder
     *         the {@link DistributedNativeBuilder} used to establish the connection.
     * @return the established {@link DistributedNativeConnectionProviderImpl}.
     * @throws AllNodesFailedException
     *         if all nodes fail to connect.
     * @throws IllegalStateException
     *         if the connection is in an illegal state.
     */
    public final DistributedNativeConnectionProviderImpl tryEstablishConnection(
            final DistributedNativeBuilder builder
    ) throws AllNodesFailedException, IllegalStateException
    {
        try
        {
            return builder.build();
        }
        catch (AllNodesFailedException | IllegalStateException e)
        {
            LOG.error("Unexpected interrupt while trying to connect to Cassandra. Reason: ", e);
            throw e;
        }
    }

    /**
     * Retrieves the CQL session associated with this connection provider.
     *
     * @return the {@link CqlSession} instance.
     */
    @Override
    public CqlSession getCqlSession()
    {
        return myDistributedNativeConnectionProviderImpl.getCqlSession();
    }

    /**
     * Retrieves the list of nodes connected by this provider.
     *
     * @return a list of {@link Node} instances.
     */
    @Override
    public List<Node> getNodes()
    {
        return myDistributedNativeConnectionProviderImpl.getNodes();
    }

    /**
     * Closes all resources and connections managed by this provider.
     *
     * @throws IOException
     *         if an I/O error occurs while closing the connections.
     */
    @Override
    public void close() throws IOException
    {
        myDistributedNativeConnectionProviderImpl.close();
    }
}