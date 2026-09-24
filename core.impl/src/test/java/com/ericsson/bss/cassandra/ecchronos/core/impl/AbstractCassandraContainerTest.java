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
import java.time.Duration;

import java.util.Map;
import java.util.UUID;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
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

        // The default driver request timeout (2s) is too tight for schema (DDL) and CAS
        // (LOCAL_SERIAL/SERIAL) operations against a single-node container that may be slow
        // or under load in CI. This causes DriverTimeoutException (PT2S), schema-agreement
        // races ("Unknown CF"/"table does not exist") and, once the node is marked down,
        // cascading "No connection was available" failures. Raise the relevant timeouts so
        // tests fail on real logic issues rather than transient timing.
        //
        // Additionally, tighten the reconnection policy. With the default exponential backoff
        // (base 1s, max 60s), a single-node container that is briefly marked down can stay
        // "down" for the rest of the test run, causing every subsequent test to fail in its
        // @Before with NodeUnavailableException. A short, bounded backoff lets a transient
        // blip recover within the run instead of cascading.
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
                .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(5))
                .build();

        mySession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIpAddress, containerPort))
                .withLocalDatacenter("DC1")
                .withConfigLoader(configLoader)
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
