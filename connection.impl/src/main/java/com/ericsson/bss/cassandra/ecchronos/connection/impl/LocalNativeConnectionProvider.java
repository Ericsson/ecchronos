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

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class LocalNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(LocalNativeConnectionProvider.class);
    private static final List<String> NODE_METRICS = Arrays.asList("pool.open-connections", "pool.available-streams",
            "pool.in-flight", "pool.orphaned-streams", "bytes-sent", "bytes-received", "cql-messages",
            "errors.request.unsent", "errors.request.aborted", "errors.request.write-timeouts",
            "errors.request.read-timeouts", "errors.request.unavailables", "errors.request.others", "retries.total",
            "retries.aborted", "retries.read-timeout", "retries.write-timeout", "retries.unavailable", "retries.other",
            "ignores.total", "ignores.aborted", "ignores.read-timeout", "ignores.write-timeout", "ignores.unavailable",
            "ignores.other", "speculative-executions", "errors.connection.init", "errors.connection.auth");
    private static final List<String> SESSION_METRICS = Arrays.asList("bytes-received", "bytes-sent", "connected-nodes",
            "cql-requests", "cql-client-timeouts", "cql-prepared-cache-size", "throttling.delay", "throttling.queue-size",
            "throttling.errors");

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

            //TODO DO WE EVEN NEED THIS? WHY NOT RUN ALL REQUEST LOCALLY?
            //DEFAULT LOADBALANCING POLICY IS TOKEN-AWARE, LOCAL
            /*
            LoadBalancingPolicy loadBalancingPolicy = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(initialContact.getDataCenter())
                    .build());

            if (builder.myRemoteRouting)
            {
                loadBalancingPolicy = DataCenterAwarePolicy.builder()
                        .withLocalDc(initialContact.getDataCenter())
                        .withChildPolicy(loadBalancingPolicy)
                        .build();
            }*/

            LOG.debug("Connecting to {}({}), local data center: {}", contactEndPoint, initialContact.getHostId(),
                    initialContact.getDataCenter());

            //TODO THIS DOES NOT EXIST IN NEW DRIVER
            //TODO CHECK IF NEEDED
            //EndPointFactory endPointFactory = new DefaultEndPointFactory();
            //EccEndPointFactory eccEndPointFactory = new EccEndPointFactory(contactEndPoint, initialContact.getHostId(),
            //        endPointFactory);

            CqlSessionBuilder sessionBuilder = fromBuilder(builder);
            sessionBuilder = sessionBuilder.withLocalDatacenter(initialContact.dataCenter);
            DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                    .withStringList(DefaultDriverOption.METRICS_NODE_ENABLED, NODE_METRICS)
                    .withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, SESSION_METRICS).build();
            sessionBuilder.withConfigLoader(loader);
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
