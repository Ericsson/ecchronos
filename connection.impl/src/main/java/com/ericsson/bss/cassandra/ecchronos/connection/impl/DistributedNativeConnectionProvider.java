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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwarePolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributedNativeConnectionProvider implements NativeConnectionProvider // CPD-OFF
{
    private static final List<String> SCHEMA_REFRESHED_KEYSPACES = ImmutableList.of("/.*/", "!system",
            "!system_distributed", "!system_schema", "!system_traces", "!system_views", "!system_virtual_schema");
    private static final List<String> NODE_METRICS = Arrays.asList(DefaultNodeMetric.OPEN_CONNECTIONS.getPath(),
            DefaultNodeMetric.AVAILABLE_STREAMS.getPath(), DefaultNodeMetric.IN_FLIGHT.getPath(),
            DefaultNodeMetric.ORPHANED_STREAMS.getPath(), DefaultNodeMetric.BYTES_SENT.getPath(),
            DefaultNodeMetric.BYTES_RECEIVED.getPath(), DefaultNodeMetric.CQL_MESSAGES.getPath(),
            DefaultNodeMetric.UNSENT_REQUESTS.getPath(), DefaultNodeMetric.ABORTED_REQUESTS.getPath(),
            DefaultNodeMetric.WRITE_TIMEOUTS.getPath(), DefaultNodeMetric.READ_TIMEOUTS.getPath(),
            DefaultNodeMetric.UNAVAILABLES.getPath(), DefaultNodeMetric.OTHER_ERRORS.getPath(),
            DefaultNodeMetric.RETRIES.getPath(), DefaultNodeMetric.RETRIES_ON_ABORTED.getPath(),
            DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT.getPath(), DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT.getPath(),
            DefaultNodeMetric.RETRIES_ON_UNAVAILABLE.getPath(), DefaultNodeMetric.RETRIES_ON_OTHER_ERROR.getPath(),
            DefaultNodeMetric.IGNORES.getPath(), DefaultNodeMetric.IGNORES_ON_ABORTED.getPath(),
            DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT.getPath(), DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT.getPath(),
            DefaultNodeMetric.IGNORES_ON_UNAVAILABLE.getPath(), DefaultNodeMetric.IGNORES_ON_OTHER_ERROR.getPath(),
            DefaultNodeMetric.SPECULATIVE_EXECUTIONS.getPath(), DefaultNodeMetric.CONNECTION_INIT_ERRORS.getPath(),
            DefaultNodeMetric.AUTHENTICATION_ERRORS.getPath());
    private static final List<String> SESSION_METRICS = Arrays.asList(DefaultSessionMetric.BYTES_RECEIVED.getPath(),
            DefaultSessionMetric.BYTES_SENT.getPath(), DefaultSessionMetric.CONNECTED_NODES.getPath(),
            DefaultSessionMetric.CQL_REQUESTS.getPath(), DefaultSessionMetric.CQL_CLIENT_TIMEOUTS.getPath(),
            DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_DELAY.getPath(),
            DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_ERRORS.getPath());

    private static final Logger LOG = LoggerFactory.getLogger(DistributedNativeConnectionProvider.class);
    private final CqlSession mySession;
    private final Node myLocalNode;
    private final boolean myRemoteRouting;
    private final List<Node> myNodes;

    private DistributedNativeConnectionProvider(
        final CqlSession session,
        final boolean remoteRouting,
        final List<Node> nodesList)
    {
        mySession = session;
        myRemoteRouting = remoteRouting;
        myNodes = nodesList;
        myLocalNode = null;
    }

    public List<Node> getNodes()
    {
        return myNodes;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public CqlSession getSession()
    {
        return mySession;
    }

    @Override
    public Node getLocalNode()
    {
        return myLocalNode;
    }

    @Override
    public boolean getRemoteRouting()
    {
        return myRemoteRouting;
    }

    @Override
    public void close()
    {
        mySession.close();
    }

    public enum AgentConnectionType
    {
        datacenterAware, rackAware, hostAware
    }

    public static class Builder
    {
        private static final int MAX_NODES_PER_DC = 999;
        private AgentConnectionType myType = AgentConnectionType.datacenterAware;
        private List<InetSocketAddress> myInitialContactPoints = new ArrayList<>();
        private String myLocalDatacenter = "datacenter1";

        private List<String> myDatacenterAware = new ArrayList<>();
        private List<Map<String, String>> myRackAware = new ArrayList<>();
        private List<InetSocketAddress> myHostAware = new ArrayList<>();

        private boolean myRemoteRouting = true;
        private boolean myIsMetricsEnabled = true;
        private AuthProvider myAuthProvider = null;
        private SslEngineFactory mySslEngineFactory = null;
        private SchemaChangeListener mySchemaChangeListener = null;
        private NodeStateListener myNodeStateListener = null;
        private MeterRegistry myMeterRegistry = null;

        public final Builder withInitialContactPoints(final List<InetSocketAddress> initialContactPoints)
        {
            myInitialContactPoints = initialContactPoints;
            return this;
        }

        public final Builder withAgentType(final String type)
        {
            myType = AgentConnectionType.valueOf(type);
            return this;
        }

        public final Builder withLocalDatacenter(final String localDatacenter)
        {
            myLocalDatacenter = localDatacenter;
            return this;
        }

        public final Builder withDatacenterAware(final List<String> datacentersInfo)
        {
            myDatacenterAware = datacentersInfo;
            return this;
        }

        public final Builder withRackAware(final List<Map<String, String>> racksInfo)
        {
            myRackAware = racksInfo;
            return this;
        }

        public final Builder withHostAware(final List<InetSocketAddress> hostsInfo)
        {
            myHostAware = hostsInfo;
            return this;
        }

        public final Builder withRemoteRouting(final boolean remoteRouting)
        {
            myRemoteRouting = remoteRouting;
            return this;
        }

        public final Builder withAuthProvider(final AuthProvider authProvider)
        {
            myAuthProvider = authProvider;
            return this;
        }

        public final Builder withSslEngineFactory(final SslEngineFactory sslEngineFactory)
        {
            this.mySslEngineFactory = sslEngineFactory;
            return this;
        }

        public final Builder withSchemaChangeListener(final SchemaChangeListener schemaChangeListener)
        {
            mySchemaChangeListener = schemaChangeListener;
            return this;
        }

        public final Builder withNodeStateListener(final NodeStateListener nodeStateListener)
        {
            myNodeStateListener = nodeStateListener;
            return this;
        }

        public final Builder withMetricsEnabled(final boolean enabled)
        {
            myIsMetricsEnabled = enabled;
            return this;
        }

        public final Builder withMeterRegistry(final MeterRegistry meterRegistry)
        {
            myMeterRegistry = meterRegistry;
            return this;
        }

        public final DistributedNativeConnectionProvider build()
        {
            LOG.info("Creating Session With Initial Contact Points");
            CqlSession session = createSession(this);
            LOG.info("Requesting Nodes List");
            List<Node> nodesList = createNodesList(session);
            return new DistributedNativeConnectionProvider(
                session, myRemoteRouting, nodesList);
        }

        private List<Node> createNodesList(final CqlSession session)
        {
            List<Node> tmpNodeList = new ArrayList<>();
            switch (myType)
            {
                case datacenterAware:
                    tmpNodeList = resolveDatacenterNodes(session, myDatacenterAware);
                    return tmpNodeList;

                case rackAware:
                    tmpNodeList = resolveRackNodes(session, myRackAware);
                    return tmpNodeList;

                case hostAware:
                    tmpNodeList = resolveHostAware(session, myHostAware);
                    return tmpNodeList;

                default:
            }
            return tmpNodeList;
        }

        private CqlSession createSession(final Builder builder)
        {
            CqlSessionBuilder sessionBuilder = fromBuilder(builder);

            DriverConfigLoader driverConfigLoader = loaderBuilder(builder, sessionBuilder).build();
            LOG.debug("Driver configuration: {}", driverConfigLoader.getInitialConfig().getDefaultProfile().entrySet());
            sessionBuilder.withConfigLoader(driverConfigLoader);
            return sessionBuilder.build();
        }

        private List<Node> resolveDatacenterNodes(final CqlSession session, final List<String> datacenterNames)
        {
            Set<String> datacenterNameSet = new HashSet<>(datacenterNames);
            List<Node> nodesList = new ArrayList<>();
            Collection<Node> nodes = session.getMetadata().getNodes().values();

            for (Node node : nodes)
            {
                if (datacenterNameSet.contains(node.getDatacenter()))
                {
                    nodesList.add(node);
                }
            }
            return nodesList;
        }

        private List<Node> resolveRackNodes(final CqlSession session, final List<Map<String, String>> rackInfo)
        {
            Set<Map<String, String>> racksInfoSet = new HashSet<>(rackInfo);
            List<Node> nodesList = new ArrayList<>();
            Collection<Node> nodes = session.getMetadata().getNodes().values();

            for (Node node : nodes)
            {
                Map<String, String> tmpRackInfo = new HashMap<>();
                tmpRackInfo.put("datacenterName", node.getDatacenter());
                tmpRackInfo.put("rackName", node.getRack());
                if (racksInfoSet.contains(tmpRackInfo))
                {
                    nodesList.add(node);
                }
            }
            return nodesList;
        }

        private List<Node> resolveHostAware(final CqlSession session, final List<InetSocketAddress> hostsInfo)
        {
            Set<InetSocketAddress> hostsInfoSet = new HashSet<>(hostsInfo);
            List<Node> nodesList = new ArrayList<>();
            Collection<Node> nodes = session.getMetadata().getNodes().values();
            for (Node node : nodes)
            {
                InetSocketAddress tmpAddress = (InetSocketAddress) node.getEndPoint().resolve();
                if (hostsInfoSet.contains(tmpAddress))
                {
                    nodesList.add(node);
                }
            }
            return nodesList;
        }

        private static ProgrammaticDriverConfigLoaderBuilder loaderBuilder(
            final Builder builder,
            final CqlSessionBuilder sessionBuilder)
        {
            ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                            SCHEMA_REFRESHED_KEYSPACES);
            if (builder.myIsMetricsEnabled)
            {
                loaderBuilder.withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, NODE_METRICS)
                        .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, SESSION_METRICS);
                loaderBuilder.withString(DefaultDriverOption.METRICS_FACTORY_CLASS, "MicrometerMetricsFactory");
                loaderBuilder.withString(DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, "TaggingMetricIdGenerator");
            }
            if (builder.myRemoteRouting)
            {
                loaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                        DataCenterAwarePolicy.class.getCanonicalName());
                loaderBuilder.withInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
                        MAX_NODES_PER_DC);
            }
            if (builder.myMeterRegistry != null)
            {
                sessionBuilder.withMetricRegistry(builder.myMeterRegistry);
            }
            return loaderBuilder;
        }

        private static CqlSessionBuilder fromBuilder(final Builder builder)
        {
            return CqlSession.builder()
                    .addContactPoints(builder.myInitialContactPoints)
                    .withLocalDatacenter(builder.myLocalDatacenter)
                    .withAuthProvider(builder.myAuthProvider)
                    .withSslEngineFactory(builder.mySslEngineFactory)
                    .withSchemaChangeListener(builder.mySchemaChangeListener)
                    .withNodeStateListener(builder.myNodeStateListener);
        }

        @VisibleForTesting
        public final List<Node> testResolveDatacenterNodes(final CqlSession session, final List<String> datacenterNames)
        {
            return resolveDatacenterNodes(session, datacenterNames);
        }

        @VisibleForTesting
        public final List<Node> testResolveRackNodes(final CqlSession session, final List<Map<String, String>> rackInfo)
        {
            return resolveRackNodes(session, rackInfo);
        }

        @VisibleForTesting
        public final List<Node> testResolveHostAware(final CqlSession session, final List<InetSocketAddress> hostsInfo)
        {
            return resolveHostAware(session, hostsInfo);
        }
    }
}
