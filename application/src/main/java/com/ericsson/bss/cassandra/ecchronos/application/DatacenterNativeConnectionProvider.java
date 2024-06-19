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

// import java.net.UnknownHostException;
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
// import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;

import io.micrometer.core.instrument.MeterRegistry;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.DatacenterAwareConfig;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NativeConnection;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
// import com.ericsson.bss.cassandra.ecchronos.core.sync.EccNodesSync;

public class DatacenterNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNativeConnectionProvider.class);

    private final AgentNativeConnectionProvider myAgentNativeConnectionProvider;

    private List<Map<String, String>> dcList = new ArrayList<>();

    // private EccNodesSync myEccNodesSync;

    public DatacenterNativeConnectionProvider(
            final Config config,
            final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
            final CertificateHandler certificateHandler,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final MeterRegistry meterRegistry,
            final DatacenterAwareConfig datacenterAwareConfig,
            final StatementDecorator statementDecorator)
    {
        NativeConnection nativeConfig = config.getConnectionConfig().getCqlConnection();
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

        String dcName = "";
        for (DatacenterAwareConfig.Datacenter datacenter : datacenterAwareConfig.getDatacenterConfig().values())
        {
            dcName = datacenter.getName();
            LOG.info("Establishing first connection with Datacenter: {}.", dcName);
            for (DatacenterAwareConfig.Host host : datacenter.getHosts())
            {
                String hostIp = host.getHost();
                Integer port = host.getPort();
                LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}",
                        hostIp, port, authEnabled, tlsEnabled);
                convertFromDCObjectToMap(dcList, dcName, hostIp, port);
            }
        }
        AgentNativeConnectionProvider.Builder nativeConnectionBuilder = AgentNativeConnectionProvider.builder()
                .withLocalDatacenter(dcName)
                .withDatacenterList(dcList)
                .withRemoteRouting(remoteRouting)
                .withAuthProvider(authProvider)
                .withSslEngineFactory(sslEngineFactory)
                .withMetricsEnabled(config.getStatisticsConfig().isEnabled())
                .withMeterRegistry(meterRegistry)
                .withSchemaChangeListener(defaultRepairConfigurationProvider)
                .withNodeStateListener(defaultRepairConfigurationProvider);

        myAgentNativeConnectionProvider = tryEstablishConnection(nativeConnectionBuilder);

        // myEccNodesSync = new EccNodesSync.Builder()
        //         .withSession(myAgentNativeConnectionProvider.getSession())
        //         .withStatementDecorator(statementDecorator).build();
        // declareNodeStatus();
    }

    public final AgentNativeConnectionProvider tryEstablishConnection(
        final AgentNativeConnectionProvider.Builder builder)
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

    // public final void declareNodeStatus()
    // {
    //     List<EndPoint> endpointList = myAgentNativeConnectionProvider.getEndPoints();
    //     Map<String, Node> nodeMap = myAgentNativeConnectionProvider.getNodes();
    //     for (EndPoint endpoint : endpointList)
    //     {
    //         try
    //         {
    //             Node node = nodeMap.get(endpoint.toString());
    //             myEccNodesSync.acquireNode(node.getEndPoint().toString(), node);
    //         }
    //         catch (UnknownHostException e)
    //         {
    //             e.printStackTrace();
    //         }
    //     }
    // }

    public static void convertFromDCObjectToMap(
        final List<Map<String, String>> dcList,
        final String dcName,
        final String host,
        final Integer port)
    {
        Map<String, String> hostPoint = new HashMap<>();
        hostPoint.put("dc", dcName);
        hostPoint.put("host", host);
        hostPoint.put("port", port.toString());

        dcList.add(hostPoint);
    }

    @Override
    public final CqlSession getSession()
    {
        return myAgentNativeConnectionProvider.getSession();
    }

    @Override
    public final Node getLocalNode()
    {
        return myAgentNativeConnectionProvider.getLocalNode();
    }

    @Override
    public final boolean getRemoteRouting()
    {
        return myAgentNativeConnectionProvider.getRemoteRouting();
    }

    @Override
    public final void close()
    {
        myAgentNativeConnectionProvider.close();
    }
}