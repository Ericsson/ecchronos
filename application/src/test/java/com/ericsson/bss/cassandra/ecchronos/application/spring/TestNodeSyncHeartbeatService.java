/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.DistributedNativeConnection;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NodesSyncHeartbeatConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestNodeSyncHeartbeatService
{
    @Mock
    private EccNodesSync eccNodesSync;
    @Mock
    private Config config;
    @Mock
    private DistributedNativeConnectionProvider nativeConnectionProvider;

    private final Map<UUID, Node> topology = new HashMap<>();

    private NodeSyncHeartbeatService service;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DistributedNativeConnection cqlConnection = mock(DistributedNativeConnection.class);
        NodesSyncHeartbeatConfig heartbeatConfig = new NodesSyncHeartbeatConfig();
        heartbeatConfig.setUnit("seconds");
        heartbeatConfig.setInterval(60L);
        heartbeatConfig.setTtl(180L); // TTL 180s (3x interval), same unit as interval

        when(config.getConnectionConfig()).thenReturn(connectionConfig);
        when(connectionConfig.getCqlConnection()).thenReturn(cqlConnection);
        when(cqlConnection.getNodesSyncHeartbeat()).thenReturn(heartbeatConfig);
        when(nativeConnectionProvider.getNodes()).thenReturn(topology);

        service = new NodeSyncHeartbeatService(config, eccNodesSync, nativeConnectionProvider);
    }

    private Row row(final String dc, final String endpoint, final String status, final UUID nodeId)
    {
        Row row = mock(Row.class);
        when(row.getString("datacenter_name")).thenReturn(dc);
        when(row.getString("node_endpoint")).thenReturn(endpoint);
        when(row.getString("node_status")).thenReturn(status);
        when(row.getUuid("node_id")).thenReturn(nodeId);
        return row;
    }

    private void inTopology(final UUID... nodeIds)
    {
        for (UUID id : nodeIds)
        {
            topology.put(id, mock(Node.class));
        }
    }

    @Test
    void heartbeatRenewsEveryOwnedRowPreservingStatusWithDerivedTtl()
    {
        UUID node1 = UUID.randomUUID();
        UUID node2 = UUID.randomUUID();
        inTopology(node1, node2);

        Row row1 = row("dc1", "10.0.0.1", "AVAILABLE", node1);
        Row row2 = row("dc1", "10.0.0.2", "UNAVAILABLE", node2);

        ResultSet rs = mock(ResultSet.class);
        when(rs.iterator()).thenReturn(Arrays.asList(row1, row2).iterator());
        when(eccNodesSync.getAllByLocalInstance()).thenReturn(rs);

        service.heartbeat();

        // Each row renewed, status preserved, TTL 180 (3x 60s).
        verify(eccNodesSync).updateNodeHeartbeat(eq(NodeStatus.AVAILABLE), eq("dc1"), eq("10.0.0.1"), eq(node1), eq(180));
        verify(eccNodesSync).updateNodeHeartbeat(eq(NodeStatus.UNAVAILABLE), eq("dc1"), eq("10.0.0.2"), eq(node2), eq(180));
    }

    @Test
    void heartbeatSkipsRowsForNodesNoLongerInTopology()
    {
        UUID present = UUID.randomUUID();
        UUID removed = UUID.randomUUID();
        // Only 'present' is still in the topology; 'removed' was deleted concurrently.
        inTopology(present);

        Row presentRow = row("dc1", "10.0.0.1", "AVAILABLE", present);
        Row removedRow = row("dc1", "10.0.0.2", "AVAILABLE", removed);

        ResultSet rs = mock(ResultSet.class);
        when(rs.iterator()).thenReturn(Arrays.asList(presentRow, removedRow).iterator());
        when(eccNodesSync.getAllByLocalInstance()).thenReturn(rs);

        service.heartbeat();

        // The present node is renewed; the removed node's row is NOT rewritten (no resurrection).
        verify(eccNodesSync).updateNodeHeartbeat(eq(NodeStatus.AVAILABLE), eq("dc1"), eq("10.0.0.1"), eq(present), eq(180));
        verify(eccNodesSync, never())
                .updateNodeHeartbeat(any(), anyString(), anyString(), eq(removed), anyInt());
    }

    @Test
    void heartbeatWithNoRowsDoesNothing()
    {
        ResultSet rs = mock(ResultSet.class);
        when(rs.iterator()).thenReturn(Collections.emptyIterator());
        when(eccNodesSync.getAllByLocalInstance()).thenReturn(rs);

        service.heartbeat();

        verify(eccNodesSync, times(0))
                .updateNodeHeartbeat(any(), anyString(), anyString(), any(), anyInt());
    }
}
