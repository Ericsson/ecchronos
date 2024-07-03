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
package com.ericsson.bss.cassandra.ecchronos.core.sync;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.eq;

public class EccNodesSyncTest
{
    @Mock
    private CqlSession mockSession;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private BoundStatement mockBoundStatement;

    @Mock
    private StatementDecorator mockStatementDecorator;

    @Mock
    private Node mockNode;

    @Mock
    private ResultSet mockResultSet;

    private EccNodesSync eccNodesSync;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);

        when(mockSession.prepare(any(SimpleStatement.class))).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(mockBoundStatement);
        when(mockStatementDecorator.apply(any(BoundStatement.class))).thenReturn(mockBoundStatement);
        when(mockSession.execute(any(BoundStatement.class))).thenReturn(mockResultSet);
        when(mockNode.getDatacenter()).thenReturn("datacenter1");
        when(mockNode.getState()).thenReturn(NodeState.UP);
        when(mockNode.getHostId()).thenReturn(UUID.randomUUID());

        EccNodesSync.Builder builder = EccNodesSync.newBuilder()
                .withSession(mockSession)
                .withStatementDecorator(mockStatementDecorator);

        eccNodesSync = builder.build();


    }

    @Test
    public void testAcquireNode() throws UnknownHostException
    {
        String endpoint = "127.0.0.1";
        ResultSet result = eccNodesSync.acquireNode(endpoint, mockNode);

        assertNotNull(result);
        verify(mockSession, times(1)).prepare(any(SimpleStatement.class));
        verify(mockPreparedStatement, times(1)).bind(any(), any(), any(), any(), any(), any(), any());
        verify(mockSession, times(1)).execute(any(BoundStatement.class));
    }

    @Test
    public void testInsertNodeInfo()
    {
        String ecchronosID = "ecchronos-test";
        String datacenterName = "datacenter1";
        String nodeEndpoint = "127.0.0.1";
        String nodeStatus = "UP";
        Instant lastConnection = Instant.now();
        Instant nextConnection = lastConnection.plus(30, ChronoUnit.MINUTES);
        UUID nodeID = UUID.randomUUID();

        ResultSet result = eccNodesSync.insertNodeInfo(ecchronosID, datacenterName, nodeEndpoint,
                nodeStatus, lastConnection, nextConnection, nodeID);
        assertNotNull(result);
        verify(mockPreparedStatement, times(1)).bind(eq(ecchronosID), eq(datacenterName),
                eq(nodeEndpoint), eq(nodeStatus),eq(lastConnection), eq(nextConnection), eq(nodeID));
        verify(mockSession, times(1)).execute(any(BoundStatement.class));
    }
}
