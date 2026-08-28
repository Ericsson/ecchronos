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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.NodeSyncState;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestStateManagementRESTImpl
{
    @Mock
    private EccNodesSync myEccNodesSync;

    private StateManagementREST stateManagementREST;

    private final UUID nodeId = UUID.randomUUID();

    @Before
    public void setupMocks()
    {
        stateManagementREST = new StateManagementRESTImpl(myEccNodesSync);
    }

    @Test
    public void testGetNodesReturnsLocalInstanceNodes()
    {
        ResultSet resultSet = mock(ResultSet.class);
        Row row = mockRow("ecc-1", "datacenter1", nodeId, "AVAILABLE");
        when(myEccNodesSync.getAllByLocalInstance()).thenReturn(resultSet);
        doAnswer(invocation ->
        {
            Consumer<Row> consumer = invocation.getArgument(0);
            consumer.accept(row);
            return null;
        }).when(resultSet).forEach(any());

        ResponseEntity<List<NodeSyncState>> response = stateManagementREST.getNodes(null, false);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(1);
        assertThat(response.getBody().get(0).ecchronosId()).isEqualTo("ecc-1");
        verify(myEccNodesSync).getAllByLocalInstance();
        verify(myEccNodesSync, never()).getAll();
    }

    @Test
    public void testGetNodesWithAllReturnsAllInstances()
    {
        ResultSet resultSet = mock(ResultSet.class);
        UUID nodeId2 = UUID.randomUUID();
        Row row1 = mockRow("ecc-1", "datacenter1", nodeId, "AVAILABLE");
        Row row2 = mockRow("ecc-2", "datacenter2", nodeId2, "UNAVAILABLE");
        when(myEccNodesSync.getAll()).thenReturn(resultSet);
        doAnswer(invocation ->
        {
            Consumer<Row> consumer = invocation.getArgument(0);
            consumer.accept(row1);
            consumer.accept(row2);
            return null;
        }).when(resultSet).forEach(any());

        ResponseEntity<List<NodeSyncState>> response = stateManagementREST.getNodes(null, true);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(2);
        assertThat(response.getBody().get(0).ecchronosId()).isEqualTo("ecc-1");
        assertThat(response.getBody().get(1).ecchronosId()).isEqualTo("ecc-2");
        verify(myEccNodesSync).getAll();
        verify(myEccNodesSync, never()).getAllByLocalInstance();
    }

    @Test
    public void testGetNodesWithAllReturnsEmptyList()
    {
        ResultSet resultSet = mock(ResultSet.class);
        when(myEccNodesSync.getAll()).thenReturn(resultSet);

        ResponseEntity<List<NodeSyncState>> response = stateManagementREST.getNodes(null, true);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEmpty();
    }

    private Row mockRow(
            final String ecchronosId,
            final String datacenterName,
            final UUID nodeIdValue,
            final String nodeStatus
    )
    {
        Row row = mock(Row.class);
        Instant now = Instant.now();
        when(row.getString(NodeSyncState.COLUMN_ECCHRONOS_ID)).thenReturn(ecchronosId);
        when(row.getString(NodeSyncState.COLUMN_DATACENTER_NAME)).thenReturn(datacenterName);
        when(row.getUuid(NodeSyncState.COLUMN_NODE_ID)).thenReturn(nodeIdValue);
        when(row.getInstant(NodeSyncState.COLUMN_LAST_CONNECTION)).thenReturn(now);
        when(row.getInstant(NodeSyncState.COLUMN_NEXT_CONNECTION)).thenReturn(now);
        when(row.getString(NodeSyncState.COLUMN_NODE_ENDPOINT)).thenReturn("/127.0.0.1:9042");
        when(row.getString(NodeSyncState.COLUMN_NODE_STATUS)).thenReturn(nodeStatus);
        return row;
    }
}
