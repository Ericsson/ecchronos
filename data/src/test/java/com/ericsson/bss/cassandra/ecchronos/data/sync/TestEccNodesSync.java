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
package com.ericsson.bss.cassandra.ecchronos.data.sync;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.utils.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@NotThreadSafe
public class TestEccNodesSync extends AbstractCassandraTest
{
    private static final String ECCHRONOS_KEYSPACE = "ecchronos";

    private EccNodesSync eccNodesSync;
    private final DistributedNativeConnectionProvider nativeConnectionProvider = getNativeConnectionProvider();
    private final Map<UUID, Node> nodesList = getNativeConnectionProvider().getNodes();
    private final UUID nodeID = UUID.randomUUID();
    private final String datacenterName = "datacenter1";

    @Before
    public void setup() throws IOException
    {
        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}",
                ECCHRONOS_KEYSPACE));
        String query = String.format(
                "CREATE TABLE IF NOT EXISTS %s.nodes_sync(" +
                        "ecchronos_id TEXT, " +
                        "datacenter_name TEXT, " +
                        "node_id UUID, " +
                        "node_endpoint TEXT, " +
                        "node_status TEXT, " +
                        "last_connection TIMESTAMP, " +
                        "next_connection TIMESTAMP, " +
                        "PRIMARY KEY(ecchronos_id, datacenter_name, node_id)) " +
                        "WITH CLUSTERING ORDER BY( datacenter_name DESC, node_id DESC);",
                ECCHRONOS_KEYSPACE
        );

        mySession.execute(query);

        eccNodesSync = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(nativeConnectionProvider)
                .withConnectionDelayValue(Long.valueOf(10))
                .withConnectionDelayUnit(TimeUnit.MINUTES)
                .withEcchronosID("ecchronos-test").build();
    }

    @After
    public void testCleanup()
    {
        mySession.execute(SimpleStatement.newInstance(
                String.format("TRUNCATE %s.%s", ECCHRONOS_KEYSPACE, "nodes_sync")));
    }

    @Test
    public void testAcquireNode()
    {
        ResultSet result = eccNodesSync.verifyAcquireNode(nodesList.values().stream().toList().get(0));
        assertNotNull(result);
    }

    @Test
    public void testInsertNodeInfo()
    {

        String nodeEndpoint = "127.0.0.1";
        String nodeStatus = "UP";
        Instant lastConnection = Instant.now();
        Instant nextConnection = lastConnection.plus(30, ChronoUnit.MINUTES);

        ResultSet result = eccNodesSync.verifyInsertNodeInfo(datacenterName, nodeEndpoint,
                nodeStatus, lastConnection, nextConnection, nodeID);
        assertNotNull(result);
    }

    @Test
    public void testUpdateNodeStatus()
    {
        ResultSet resultSet = eccNodesSync.updateNodeStatus(NodeStatus.AVAILABLE, datacenterName, nodeID);
        assertNotNull(resultSet);
        assertTrue(resultSet.wasApplied());
    }

    @Test
    public void testEccNodesWithNullList()
    {
        DistributedNativeConnectionProvider mockNativeConnectionProvider = mock(DistributedNativeConnectionProvider.class);
        when(mockNativeConnectionProvider.getNodes()).thenReturn(null);
        EccNodesSync.Builder tmpEccNodesSyncBuilder = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(mockNativeConnectionProvider);
        NullPointerException exception = assertThrows(
                NullPointerException.class, tmpEccNodesSyncBuilder::build);
        assertEquals("Nodes map cannot be null", exception.getMessage());
    }

    @Test
    public void testEccNodesWithNullNativeConnectionProvider()
    {
        EccNodesSync.Builder tmpEccNodesSyncBuilder = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(null);
        NullPointerException exception = assertThrows(
                NullPointerException.class, tmpEccNodesSyncBuilder::build);
        assertEquals("Native Connection cannot be null", exception.getMessage());
    }

    @Test
    public void testAcquiredNodesWithEmptyList() throws UnknownHostException
    {
        DistributedNativeConnectionProvider mockNativeConnectionProvider = mock(DistributedNativeConnectionProvider.class);
        when(mockNativeConnectionProvider.getNodes()).thenReturn(new HashMap<>());
        EccNodesSync tmpEccNodesSync = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(mockNativeConnectionProvider)
                .build();
        EcChronosException exception = assertThrows(
                EcChronosException.class, tmpEccNodesSync::acquireNodes);
        assertEquals(
                "Cannot Acquire Nodes because there is no nodes to be acquired",
                exception.getMessage());
    }

    @Test
    public void testEccNodesWithNullSession()
    {
        EccNodesSync.Builder tmpEccNodesSyncBuilder = EccNodesSync.newBuilder()
                .withSession(null)
                .withNativeConnection(nativeConnectionProvider);
        NullPointerException exception = assertThrows(
                NullPointerException.class, tmpEccNodesSyncBuilder::build);
        assertEquals("Session cannot be null", exception.getMessage());
    }

    @Test
    public void testGetAllNodes()
    {
        eccNodesSync.verifyInsertNodeInfo(datacenterName, "127.0.0.1", NodeStatus.AVAILABLE.name(), Instant.now(), Instant.now().plus(30, ChronoUnit.MINUTES), UUID.randomUUID());
        eccNodesSync.verifyInsertNodeInfo(datacenterName, "127.0.0.2", NodeStatus.UNAVAILABLE.name(), Instant.now(), Instant.now().plus(30, ChronoUnit.MINUTES), UUID.randomUUID());
        ResultSet resultSet = eccNodesSync.getResultSet();
        assertNotNull(resultSet);
    }
}
