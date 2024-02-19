/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwarePolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public final class LocalNativeConnectionProvider implements NativeConnectionProvider
{
    private static final List<String> SCHEMA_REFRESHED_KEYSPACES = ImmutableList.of("/.*/", "!system",
            "!system_distributed", "!system_schema", "!system_traces", "!system_views", "!system_virtual_schema");
    private static final Logger LOG = LoggerFactory.getLogger(LocalNativeConnectionProvider.class);
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

    public static final int DEFAULT_NATIVE_PORT = 9042;
    public static final String DEFAULT_LOCAL_HOST = "localhost";

    private final CqlSession mySession;
    private final Node myLocalNode;
    private final boolean myRemoteRouting;
    private final String mySerialConsistencyLevel;

    private LocalNativeConnectionProvider(
        final CqlSession session,
        final Node node,
        final boolean remoteRouting,
        final String serialConsistencyLevel)
    {
        mySession = session;
        myLocalNode = node;
        myRemoteRouting = remoteRouting;
        mySerialConsistencyLevel = serialConsistencyLevel;
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
    public String getSerialConsistency()
    {
        return mySerialConsistencyLevel;
    }

    @Override
    public void close()
    {
        mySession.close();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private static final int MAX_NODES_PER_DC = 999;

        private String myLocalhost = DEFAULT_LOCAL_HOST;
        private int myPort = DEFAULT_NATIVE_PORT;
        private boolean myRemoteRouting = true;
        private String mySerialConsistency = "DEFAULT";
        private boolean myIsMetricsEnabled = true;
        private AuthProvider myAuthProvider = null;
        private SslEngineFactory mySslEngineFactory = null;
        private SchemaChangeListener mySchemaChangeListener = null;
        private MeterRegistry myMeterRegistry = null;

        public final Builder withLocalhost(final String localhost)
        {
            myLocalhost = localhost;
            return this;
        }

        public final Builder withPort(final int port)
        {
            myPort = port;
            return this;
        }

        public final Builder withRemoteRouting(final boolean remoteRouting)
        {
            myRemoteRouting = remoteRouting;
            return this;
        }

        public final Builder withConsistencySerial(final String serialConsistency)
        {
            mySerialConsistency = serialConsistency;
            return this;
        }

        public final Builder withAuthProvider(final AuthProvider authProvider)
        {
            this.myAuthProvider = authProvider;
            return this;
        }

        public final Builder withSslEngineFactory(final SslEngineFactory sslEngineFactory)
        {
            this.mySslEngineFactory = sslEngineFactory;
            return this;
        }

        public final Builder withSchemaChangeListener(final SchemaChangeListener schemaChangeListener)
        {
            this.mySchemaChangeListener = schemaChangeListener;
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

        public final LocalNativeConnectionProvider build()
        {
            CqlSession session = createSession(this);
            Node node = resolveLocalhost(session, localEndPoint());
            return new LocalNativeConnectionProvider(session, node, myRemoteRouting, mySerialConsistency);
        }

        private EndPoint localEndPoint()
        {
            return new ContactEndPoint(myLocalhost, myPort);
        }

        private static CqlSession createSession(final Builder builder)
        {
            EndPoint contactEndPoint = builder.localEndPoint();

            InitialContact initialContact = resolveInitialContact(contactEndPoint, builder);

            LOG.debug("Connecting to {}({}), local data center: {}", contactEndPoint, initialContact.getHostId(),
                    initialContact.getDataCenter());

            CqlSessionBuilder sessionBuilder = fromBuilder(builder);
            sessionBuilder = sessionBuilder.withLocalDatacenter(initialContact.dataCenter);
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
            sessionBuilder.withConfigLoader(loaderBuilder.build());
            return sessionBuilder.build();
        }

        private static InitialContact resolveInitialContact(final EndPoint contactEndPoint, final Builder builder)
        {
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                            DcInferringLoadBalancingPolicy.class.getName())
                    .build();
            CqlSessionBuilder cqlSessionBuilder = fromBuilder(builder).withConfigLoader(loader);
            try (Session session = cqlSessionBuilder.build())
            {
                for (Node node : session.getMetadata().getNodes().values())
                {
                    if (node.getEndPoint().equals(contactEndPoint))
                    {
                        return new InitialContact(node.getDatacenter(), node.getHostId());
                    }
                }
            }

            throw new IllegalStateException("Unable to find local data center");
        }

        private static CqlSessionBuilder fromBuilder(final Builder builder)
        {
            return CqlSession.builder()
                    .addContactEndPoint(builder.localEndPoint())
                    .withAuthProvider(builder.myAuthProvider)
                    .withSslEngineFactory(builder.mySslEngineFactory)
                    .withSchemaChangeListener(builder.mySchemaChangeListener);
        }

        private static Node resolveLocalhost(final Session session, final EndPoint localEndpoint)
        {
            Node tmpNode = null;

            for (Node node : session.getMetadata().getNodes().values())
            {
                if (node.getEndPoint().equals(localEndpoint))
                {
                    tmpNode = node;
                }
            }

            if (tmpNode == null)
            {
                throw new IllegalArgumentException("Node " + localEndpoint + " not found among cassandra hosts");
            }

            return tmpNode;
        }
    }

    static class InitialContact
    {
        private final String dataCenter;
        private final UUID hostId;

        InitialContact(final String aDataCenter, final UUID aHostId)
        {
            this.dataCenter = aDataCenter;
            this.hostId = aHostId;
        }

        String getDataCenter()
        {
            return dataCenter;
        }

        UUID getHostId()
        {
            return hostId;
        }
    }
}
