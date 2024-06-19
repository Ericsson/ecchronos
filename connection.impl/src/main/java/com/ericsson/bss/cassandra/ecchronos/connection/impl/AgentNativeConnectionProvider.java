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
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwarePolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AgentNativeConnectionProvider implements NativeConnectionProvider
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

    private static final Logger LOG = LoggerFactory.getLogger(AgentNativeConnectionProvider.class);
    private final CqlSession mySession;
    private final Node myLocalNode;
    private final boolean myRemoteRouting;
    private final List<EndPoint> myEndPoints;
    private final Map<String, Node> myNodes;

    private AgentNativeConnectionProvider(
        final CqlSession session,
        final boolean remoteRouting,
        final Map<String, Node> nodesMap,
        final List<EndPoint> endPoints)
    {
        mySession = session;
        myRemoteRouting = remoteRouting;
        myLocalNode = null;
        myNodes = nodesMap;
        myEndPoints = endPoints;
    }

    public List<EndPoint> getEndPoints()
    {
        return myEndPoints;
    }

    public Map<String, Node>  getNodes()
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

    public static class Builder
    {
        private static final int MAX_NODES_PER_DC = 999;
        private List<Map<String, String>> myDatacenterList;
        private List<EndPoint> myEndPoints;
        private String myLocalDatacenter;

        private boolean myRemoteRouting = true;
        private boolean myIsMetricsEnabled = true;
        private AuthProvider myAuthProvider = null;
        private SslEngineFactory mySslEngineFactory = null;
        private SchemaChangeListener mySchemaChangeListener = null;
        private NodeStateListener myNodeStateListener = null;
        private MeterRegistry myMeterRegistry = null;

        public final Builder withLocalDatacenter(final String datacenterName)
        {
            myLocalDatacenter = datacenterName;
            return this;
        }

        public final Builder withDatacenterList(final List<Map<String, String>> datacenterList)
        {
            myDatacenterList = datacenterList;
            myEndPoints = createEndPointList();
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

        public final AgentNativeConnectionProvider build()
        {
            CqlSession session = createSession(this);
            Map<String, Node> nodesMap = createNodeMap(session);
            return new AgentNativeConnectionProvider(
                session, myRemoteRouting, nodesMap, myEndPoints);
        }

        private CqlSession createSession(final Builder builder)
        {
            CqlSessionBuilder sessionBuilder = fromBuilder(builder);

            DriverConfigLoader driverConfigLoader = loaderBuilder(builder, sessionBuilder).build();
            LOG.debug("Driver configuration: {}", driverConfigLoader.getInitialConfig().getDefaultProfile().entrySet());
            sessionBuilder.withConfigLoader(driverConfigLoader);
            return sessionBuilder.build();
        }

        private List<EndPoint> createEndPointList()
        {
            List<EndPoint> endPoints = new ArrayList<>();
            for (Map<String, String> host : myDatacenterList)
            {
                ContactEndPoint endPoint = new ContactEndPoint(host.get("host"), Integer.valueOf(host.get("port")));
                endPoint.resolve();
                endPoints.add(endPoint);
            }
            return endPoints;
        }

        private Map<String, Node> createNodeMap(final CqlSession session)
        {
            Map<String, Node> nodeMap = new HashMap<>();
            for (EndPoint endPoint: myEndPoints)
            {
                Node node = searchNode(endPoint.toString(), session);
                if (node != null)
                {
                    nodeMap.put(endPoint.toString(), node);
                }
                else
                {
                    nodeMap.put(endPoint.toString(), null);
                }
            }
            return nodeMap;
        }

        public final Node searchNode(
            final String endpoint,
            final CqlSession session)
        {
            Collection<Node> nodeList = session.getMetadata().getNodes().values();

            Node tmpNode = null;
            for (Node node : nodeList)
            {
                if (node.getEndPoint().toString().equals(endpoint))
                {
                    tmpNode = node;
                    return tmpNode;
                }
            }
            return tmpNode;
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
                    .addContactEndPoints(builder.myEndPoints)
                    .withLocalDatacenter(builder.myLocalDatacenter)
                    .withAuthProvider(builder.myAuthProvider)
                    .withSslEngineFactory(builder.mySslEngineFactory)
                    .withSchemaChangeListener(builder.mySchemaChangeListener)
                    .withNodeStateListener(builder.myNodeStateListener);
        }
    }
}