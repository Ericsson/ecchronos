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
package com.ericsson.bss.cassandra.ecchronos.data.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

public class AbstractCassandraTest
{
    private static final List<CassandraContainer<?>> nodes = new ArrayList<>();
    protected static CqlSession mySession;

    private static DistributedNativeConnectionProvider myNativeConnectionProvider;

    @BeforeClass
    public static void setUpCluster()
    {
        CassandraContainer<?> node = new CassandraContainer<>(DockerImageName.parse("cassandra:4.1.5"))
                .withExposedPorts(9042, 7000)
                .withEnv("CASSANDRA_DC", "DC1")
                .withEnv("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
                .withEnv("CASSANDRA_CLUSTER_NAME", "TestCluster");
        nodes.add(node);
        node.start();
        mySession = CqlSession.builder()
                .addContactPoint(node.getContactPoint())
                .withLocalDatacenter(node.getLocalDatacenter())
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
            public Map<UUID, Node> getNodes()
            {
                return nodesList;
            }

            @Override
            public void addNode(Node myNode) {
                nodesList.put(myNode.getHostId(), myNode);
            }

            @Override
            public void removeNode(Node myNode) {
                nodesList.remove(myNode.getHostId());
            }

            @Override
            public Boolean confirmNodeValid(Node node) {
                return false;
            }

            @Override
            public ConnectionType getConnectionType()
            {
                return ConnectionType.datacenterAware;
            }
        };
    }

    @AfterClass
    public static void tearDownCluster()
    {
        // Stop all nodes
        for (CassandraContainer<?> node : nodes)
        {
            node.stop();
        }
    }

    public static DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }
}
