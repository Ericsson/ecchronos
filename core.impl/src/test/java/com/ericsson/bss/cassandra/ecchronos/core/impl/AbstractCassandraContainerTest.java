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
package com.ericsson.bss.cassandra.ecchronos.core.impl;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.net.InetSocketAddress;

import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;

public class AbstractCassandraContainerTest
{
    protected static CqlSession mySession;

    private static DistributedNativeConnectionProvider myNativeConnectionProvider;
    private static CassandraContainer<?> node;

    @SuppressWarnings ("resource")
    @BeforeClass
    public static void setUpCluster()
    {
        // This is set as an environment variable ('it.cassandra.version') in maven using the '-D' flag.
        String cassandraVersion = System.getProperty("it.cassandra.version");
        if (cassandraVersion == null)
        {
            // No environment version set, just use latest.
            cassandraVersion = "latest";
        }
        node = new CassandraContainer<>(DockerImageName.parse("cassandra:" + cassandraVersion))
                .withExposedPorts(9042, 7000, 7199)
                .withEnv("CASSANDRA_DC", "DC1")
                .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
                .withEnv("CASSANDRA_CLUSTER_NAME", "TestCluster")
                .withEnv("JMX_PORT", "7199");
        node.start();
        String containerIpAddress = node.getHost();
        Integer containerPort = node.getMappedPort(9042);

        mySession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIpAddress, containerPort))
                .withLocalDatacenter("DC1")
                .build();

        Map<UUID, Node> nodesList = mySession.getMetadata().getNodes();
        myNativeConnectionProvider = new DistributedNativeConnectionProvider()
        {
            @Override
            public CqlSession getCqlSession()
            {
                return mySession;
            }

            @Override
            public Map<UUID, Node>  getNodes()
            {
                return nodesList;
            }

            @Override
            public void addNode(Node myNode)
            {
            }

            @Override
            public void removeNode(Node myNode)
            {
            }

            @Override
            public Boolean confirmNodeValid(Node node)
            {
                return false;
            }

            @Override
            public ConnectionType getConnectionType()
            {
                return ConnectionType.hostAware;
            }
        };
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (mySession != null)
        {
            mySession.close();
        }
        node.stop();
    }

    public static DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    public static CassandraContainer<?> getContainerNode()
    {
        return node;
    }
}
