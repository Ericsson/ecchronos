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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
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

    private LocalNativeConnectionProvider(Cluster cluster, Host host)
    {
        myCluster = cluster;
        mySession = cluster.connect();
        myLocalHost = host;
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

        public LocalNativeConnectionProvider build()
        {
            Cluster cluster = createCluster(this);
            Host host = resolveLocalhost(cluster, myLocalhost);

            return new LocalNativeConnectionProvider(cluster, host);
        }

        private static Cluster createCluster(Builder builder)
        {
            String localhost = builder.myLocalhost;
            int port = builder.myPort;

            InetSocketAddress hostAddress = new InetSocketAddress(localhost, port);

            String localDataCenter = resolveLocalDataCenter(hostAddress);

            LoadBalancingPolicy loadBalancingPolicy = DataCenterAwarePolicy.builder()
                    .withLocalDc(localDataCenter)
                    .withChildPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder()
                            .withLocalDc(localDataCenter)
                            .build()))
                    .build();

            LOG.debug("Connecting to {}, local data center: {}", localhost, localDataCenter);

            return Cluster.builder()
                    .addContactPointsWithPorts(hostAddress)
                    .withLoadBalancingPolicy(loadBalancingPolicy)
                    .build();
        }

        private static String resolveLocalDataCenter(InetSocketAddress hostAddress)
        {
            try (Cluster cluster = Cluster.builder().addContactPointsWithPorts(hostAddress).build())
            {
                InetAddress contactAddress = hostAddress.getAddress();

                for (Host host : cluster.getMetadata().getAllHosts())
                {
                    if (contactAddress.equals(host.getAddress()))
                    {
                        String dataCenter = host.getDatacenter();

                        if (dataCenter != null)
                        {
                            return dataCenter;
                        }
                    }
                }
            }

            throw new IllegalStateException("Unable to find local data center");
        }

        private static Host resolveLocalhost(Cluster cluster, String localhost)
        {
            Host tmpHost = null;

            try
            {
                InetAddress localhostAddress = InetAddress.getByName(localhost);

                for (Host host : cluster.getMetadata().getAllHosts())
                {
                    if (host.getAddress().equals(localhostAddress))
                    {
                        tmpHost = host;
                    }
                }
            }
            catch (UnknownHostException e)
            {
                throw new IllegalArgumentException(e);
            }

            if (tmpHost == null)
            {
                throw new IllegalArgumentException("Host " + localhost + " not found among cassandra hosts");
            }

            return tmpHost;
        }
    }
}
