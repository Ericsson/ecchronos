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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;

import io.micrometer.core.instrument.MeterRegistry;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.AgentConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NativeConnection;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;

public class AgentNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(AgentNativeConnectionProvider.class);

    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;

    public AgentNativeConnectionProvider(
            final Config config,
            final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
            final CertificateHandler certificateHandler,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final MeterRegistry meterRegistry)
    {
        NativeConnection nativeConfig = config.getConnectionConfig().getCqlConnection();
        AgentConnectionConfig agentConnectionConfig = config.getConnectionConfig().getCqlConnection()
            .getAgentConnectionConfig();
        boolean remoteRouting = nativeConfig.getRemoteRouting();
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

        DistributedNativeConnectionProvider.Builder nativeConnectionBuilder =
            DistributedNativeConnectionProvider.builder()
                .withInitialContactPoints(resolveInitialContactPoints(agentConnectionConfig.getContactPoints()))
                .withAgentType(agentConnectionConfig.getType().toString())
                .withLocalDatacenter(agentConnectionConfig.getLocalDatacenter())
                .withRemoteRouting(remoteRouting)
                .withAuthProvider(authProvider)
                .withSslEngineFactory(sslEngineFactory)
                .withMetricsEnabled(config.getStatisticsConfig().isEnabled())
                .withMeterRegistry(meterRegistry)
                .withSchemaChangeListener(defaultRepairConfigurationProvider)
                .withNodeStateListener(defaultRepairConfigurationProvider);

        LOG.info("Preparing Agent Connection Config");
        nativeConnectionBuilder = resolveAgentProviderBuilder(nativeConnectionBuilder, agentConnectionConfig);
        LOG.info("Establishing Connection With Nodes");
        myDistributedNativeConnectionProvider = tryEstablishConnection(nativeConnectionBuilder);
    }

    public final DistributedNativeConnectionProvider.Builder resolveAgentProviderBuilder(
        final DistributedNativeConnectionProvider.Builder builder,
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

    public final DistributedNativeConnectionProvider tryEstablishConnection(
        final DistributedNativeConnectionProvider.Builder builder)
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

    @Override
    public final CqlSession getSession()
    {
        return myDistributedNativeConnectionProvider.getSession();
    }

    @Override
    public final Node getLocalNode()
    {
        return myDistributedNativeConnectionProvider.getLocalNode();
    }

    @Override
    public final boolean getRemoteRouting()
    {
        return myDistributedNativeConnectionProvider.getRemoteRouting();
    }

    @Override
    public final void close()
    {
        myDistributedNativeConnectionProvider.close();
    }
}
