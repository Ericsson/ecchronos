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

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DefaultEndPointFactory;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.EndPointFactory;
import com.datastax.driver.core.ExtendedAuthProvider;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwarePolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;

public class LocalNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(LocalNativeConnectionProvider.class);

    public static final int DEFAULT_NATIVE_PORT = 9042;
    public static final String DEFAULT_LOCAL_HOST = "localhost";

    private final Cluster myCluster;
    private final Session mySession;
    private final Host myLocalHost;
    private final boolean myRemoteRouting;

    private LocalNativeConnectionProvider(Cluster cluster, Host host, boolean remoteRouting)
    {
        myCluster = cluster;
        mySession = cluster.connect();
        myLocalHost = host;
        myRemoteRouting = remoteRouting;
    }

    @Override
    public Session getSession()
    {
        return mySession;
    }

    @Override
    public Host getLocalHost()
    {
        return myLocalHost;
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
        myCluster.close();
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
        private AuthProvider authProvider = AuthProvider.NONE;
        private SSLOptions sslOptions = null;

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

        public Builder withAuthProvider(ExtendedAuthProvider authProvider)
        {
            this.authProvider = authProvider;
            return this;
        }

        public Builder withSslOptions(SSLOptions sslOptions)
        {
            this.sslOptions = sslOptions;
            return this;
        }

        public LocalNativeConnectionProvider build()
        {
            Cluster cluster = createCluster(this);
            cluster.getConfiguration().getCodecRegistry().register(SimpleTimestampCodec.instance);
            Host host = resolveLocalhost(cluster, localEndPoint());

            return new LocalNativeConnectionProvider(cluster, host, myRemoteRouting);
        }

        private EndPoint localEndPoint()
        {
            return new ContactEndPoint(myLocalhost, myPort);
        }

        private static Cluster createCluster(Builder builder)
        {
            EndPoint contactEndPoint = builder.localEndPoint();

            InitialContact initialContact = resolveInitialContact(contactEndPoint, builder);

            LoadBalancingPolicy loadBalancingPolicy = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(initialContact.getDataCenter())
                    .build());

            if (builder.myRemoteRouting)
            {
                loadBalancingPolicy = DataCenterAwarePolicy.builder()
                        .withLocalDc(initialContact.getDataCenter())
                        .withChildPolicy(loadBalancingPolicy)
                        .build();
            }

            LOG.debug("Connecting to {}({}), local data center: {}", contactEndPoint, initialContact.getHostId(),
                    initialContact.getDataCenter());

            EndPointFactory endPointFactory = new DefaultEndPointFactory();
            EccEndPointFactory eccEndPointFactory = new EccEndPointFactory(contactEndPoint, initialContact.getHostId(),
                    endPointFactory);

            Cluster cluster = fromBuilder(builder)
                    .withEndPointFactory(eccEndPointFactory)
                    .withLoadBalancingPolicy(loadBalancingPolicy)
                    .build();
            cluster.register(eccEndPointFactory);
            return cluster;
        }

        private static InitialContact resolveInitialContact(EndPoint contactEndPoint, Builder builder)
        {
            try (Cluster cluster = fromBuilder(builder).build())
            {
                for (Host host : cluster.getMetadata().getAllHosts())
                {
                    if (host.getEndPoint().equals(contactEndPoint))
                    {
                        return new InitialContact(host.getDatacenter(), host.getHostId());
                    }
                }
            }

            throw new IllegalStateException("Unable to find local data center");
        }

        private static Cluster.Builder fromBuilder(Builder builder)
        {
            return Cluster.builder()
                    .addContactPoint(builder.localEndPoint())
                    .withAuthProvider(builder.authProvider)
                    .withSSL(builder.sslOptions);
        }

        private static Host resolveLocalhost(Cluster cluster, EndPoint localEndpoint)
        {
            Host tmpHost = null;

            for (Host host : cluster.getMetadata().getAllHosts())
            {
                if (host.getEndPoint().equals(localEndpoint))
                {
                    tmpHost = host;
                }
            }

            if (tmpHost == null)
            {
                throw new IllegalArgumentException("Host " + localEndpoint + " not found among cassandra hosts");
            }

            return tmpHost;
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
