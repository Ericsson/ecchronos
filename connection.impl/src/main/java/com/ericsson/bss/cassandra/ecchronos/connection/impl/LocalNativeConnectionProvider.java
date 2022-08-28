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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class LocalNativeConnectionProvider implements NativeConnectionProvider
{
    private static final List<String> SCHEMA_REFRESHED_KEYSPACES = ImmutableList.of("/.*/", "!system",
            "!system_distributed", "!system_schema", "!system_traces");
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

    private LocalNativeConnectionProvider(CqlSession session, Node node, boolean remoteRouting)
    {
        mySession = session;
        myLocalNode = node;
        myRemoteRouting = remoteRouting;
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String myLocalhost = DEFAULT_LOCAL_HOST;
        private int myPort = DEFAULT_NATIVE_PORT;
        private boolean myRemoteRouting = true;
        private AuthProvider authProvider = null;
        private SslEngineFactory sslEngineFactory = null;
        private SchemaChangeListener schemaChangeListener = null;

        public Builder withLocalhost(String localhost)
        {
            myLocalhost = localhost;
            return this;
        }

        public Builder withPort(int port)
        {
            myPort = port;
            return this;
        }

        public Builder withRemoteRouting(boolean remoteRouting)
        {
            myRemoteRouting = remoteRouting;
            return this;
        }

        public Builder withAuthProvider(AuthProvider authProvider)
        {
            this.authProvider = authProvider;
            return this;
        }

        public Builder withSslEngineFactory(SslEngineFactory sslEngineFactory)
        {
            this.sslEngineFactory = sslEngineFactory;
            return this;
        }

        public Builder withSchemaChangeListener(SchemaChangeListener schemaChangeListener)
        {
            this.schemaChangeListener = schemaChangeListener;
            return this;
        }

        public LocalNativeConnectionProvider build()
        {
            CqlSession session = createSession(this);
            Node node = resolveLocalhost(session, localEndPoint());
            return new LocalNativeConnectionProvider(session, node, myRemoteRouting);
        }

        private EndPoint localEndPoint()
        {
            return new ContactEndPoint(myLocalhost, myPort);
        }

        private static CqlSession createSession(Builder builder)
        {
            EndPoint contactEndPoint = builder.localEndPoint();

            InitialContact initialContact = resolveInitialContact(contactEndPoint, builder);

            LOG.debug("Connecting to {}({}), local data center: {}", contactEndPoint, initialContact.getHostId(),
                    initialContact.getDataCenter());

            CqlSessionBuilder sessionBuilder = fromBuilder(builder);
            sessionBuilder = sessionBuilder.withLocalDatacenter(initialContact.dataCenter);
            ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, NODE_METRICS)
                    .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, SESSION_METRICS)
                    .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, SCHEMA_REFRESHED_KEYSPACES);
            if (builder.myRemoteRouting)
            {
                loaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DataCenterAwarePolicy.class.getCanonicalName());
                loaderBuilder.withInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC, 999);
            }
            sessionBuilder.withConfigLoader(loaderBuilder.build());
            return sessionBuilder.build();
        }

        private static InitialContact resolveInitialContact(EndPoint contactEndPoint, Builder builder)
        {
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class.getName())
                    .build();
            CqlSessionBuilder cqlSessionBuilder = fromBuilder(builder).withConfigLoader(loader);
            try(Session session = cqlSessionBuilder.build())
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

        private static CqlSessionBuilder fromBuilder(Builder builder)
        {
            return CqlSession.builder()
                    .addContactEndPoint(builder.localEndPoint())
                    .withAuthProvider(builder.authProvider)
                    .withSslEngineFactory(builder.sslEngineFactory)
                    .withSchemaChangeListener(builder.schemaChangeListener);
        }

        private static Node resolveLocalhost(Session session, EndPoint localEndpoint)
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

        InitialContact(String dataCenter, UUID hostId)
        {
            this.dataCenter = dataCenter;
            this.hostId = hostId;
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
