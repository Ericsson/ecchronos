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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.DistributedJmxConnection;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.RetryPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.enums.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RetrySchedulerServiceTest
{
    @Mock
    private EccNodesSync eccNodesSync;
    @Mock
    private Config config;
    @Mock
    private DistributedJmxConnectionProvider jmxConnectionProvider;
    @Mock
    private DistributedNativeConnectionProvider nativeConnectionProvider;
    @Mock
    private RetryPolicyConfig retryPolicyConfig;
    @Mock
    private RetryPolicyConfig.RetryDelay retryDelay;
    @Mock
    private RetryPolicyConfig.RetrySchedule retrySchedule;
    private RetrySchedulerService retrySchedulerService;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.openMocks(this);

        // Mock nested method calls for Config
        ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
        DistributedJmxConnection jmxConnection = mock(DistributedJmxConnection.class);

        when(config.getConnectionConfig()).thenReturn(connectionConfig);
        when(connectionConfig.getJmxConnection()).thenReturn(jmxConnection);
        when(retryDelay.getStartDelay()).thenReturn(500L);
        when(retryDelay.getMaxDelay()).thenReturn(5000L);
        when(jmxConnection.getRetryPolicyConfig()).thenReturn(retryPolicyConfig);

        // Mock behavior for retryPolicyConfig
        when(retrySchedule.getInitialDelay()).thenReturn(1000L);
        when(retrySchedule.getFixedDelay()).thenReturn(1000L);
        when(retryPolicyConfig.getMaxAttempts()).thenReturn(3);
        when(retryPolicyConfig.getRetryDelay()).thenReturn(retryDelay);
        when(retryPolicyConfig.getRetrySchedule()).thenReturn(retrySchedule);

        // Initialize RetrySchedulerService with new RetryBackoffStrategy
        retrySchedulerService = new RetrySchedulerService(eccNodesSync, config, jmxConnectionProvider, nativeConnectionProvider);
    }

    @Test
    void testSchedulerStart()
    {
        // Call the method under test
        retrySchedulerService.startScheduler();
        // Verify that the scheduler starts correctly with the correct initialDelay and fixedDelay
        verify(retrySchedule, times(1)).getInitialDelay();
        verify(retrySchedule, times(1)).getFixedDelay();
    }

    @Test
    void testRetryNodesWhenNoUnavailableNodes()
    {
        // Mock ResultSet and Row to simulate no unavailable nodes
        ResultSet mockResultSet = mock(ResultSet.class);
        when(eccNodesSync.getResultSet()).thenReturn(mockResultSet);
        when(mockResultSet.iterator()).thenReturn(Collections.emptyIterator());

        retrySchedulerService.retryNodes();

        // Verify that no nodes are marked as UNAVAILABLE or retried
        verify(eccNodesSync, never()).updateNodeStatus(any(), anyString(), any());
        verify(jmxConnectionProvider, never()).getJmxConnector(any());
    }

    @Test
    void testRetryNodesWithUnavailableNodeWhenConnectionSuccessful() throws IOException
    {
        // Create mock objects
        ResultSet mockResultSet = mock(ResultSet.class);
        Row mockRow = mock(Row.class);
        UUID nodeId = UUID.randomUUID();

        // Setup mock behavior for ResultSet and Row
        when(mockRow.getString("node_status")).thenReturn(NodeStatus.UNAVAILABLE.name());
        when(mockRow.getUuid("node_id")).thenReturn(nodeId);
        when(mockResultSet.iterator()).thenReturn(Collections.singletonList(mockRow).iterator());
        when(eccNodesSync.getResultSet()).thenReturn(mockResultSet);

        // Mock Node and its behavior
        Node mockNode = mock(Node.class);
        when(mockNode.getHostId()).thenReturn(nodeId);
        when(mockNode.getDatacenter()).thenReturn("datacenter");
        when(nativeConnectionProvider.getNodes()).thenReturn(Collections.singletonList(mockNode));

        // Mock JMX connector behavior
        JMXConnector mockJmxConnector = mock(JMXConnector.class);
        when(jmxConnectionProvider.getJmxConnector(nodeId)).thenReturn(mockJmxConnector);
        when(mockJmxConnector.getConnectionId()).thenReturn("connected");

        // Mock the JMX connections map
        ConcurrentHashMap<UUID, JMXConnector> mockJmxConnections = mock(ConcurrentHashMap.class);
        when(jmxConnectionProvider.getJmxConnections()).thenReturn(mockJmxConnections);

        // Call the method under test
        retrySchedulerService.retryNodes();

        // Verify interactions
        // Ensure the status update call is made with expected parameters
        verify(eccNodesSync).updateNodeStatus(NodeStatus.AVAILABLE, "datacenter", nodeId);

        // Verify JMXConnector is added to the map
        verify(mockJmxConnections).put(eq(nodeId), any(JMXConnector.class));
    }

    @Test
    void testRetryNodesWithUnavailableNodeWhenConnectionFailed() throws IOException
    {
        // Mock the node as unavailable
        ResultSet mockResultSet = mock(ResultSet.class);
        when(eccNodesSync.getResultSet()).thenReturn(mockResultSet);
        Row mockRow = mock(Row.class);
        when(mockRow.getString("node_status")).thenReturn("UNAVAILABLE");
        UUID nodeId = UUID.randomUUID();
        when(mockRow.getUuid("node_id")).thenReturn(nodeId);
        when(mockResultSet.iterator()).thenReturn(Collections.singletonList(mockRow).iterator());

        // Mock the nodes list to contain the unavailable node
        Node mockNode = mock(Node.class);
        when(mockNode.getHostId()).thenReturn(nodeId);
        when(mockNode.getDatacenter()).thenReturn("datacenter");
        when(nativeConnectionProvider.getNodes()).thenReturn(Collections.singletonList(mockNode));

        // Mock the JMX connection behavior for a failed reconnection
        when(jmxConnectionProvider.getJmxConnector(nodeId)).thenReturn(null);

        retrySchedulerService.retryNodes();

        // Check the node status update to UNREACHABLE
        verify(eccNodesSync).updateNodeStatus(eq(NodeStatus.UNREACHABLE), anyString(), eq(nodeId));
    }

    @Test
    void testMaxRetryAttemptsReached() throws IOException
    {
        // Mock the node as unavailable
        ResultSet mockResultSet = mock(ResultSet.class);
        when(eccNodesSync.getResultSet()).thenReturn(mockResultSet);
        Row mockRow = mock(Row.class);
        when(mockRow.getString("node_status")).thenReturn("UNAVAILABLE");
        UUID nodeId = UUID.randomUUID();
        when(mockRow.getUuid("node_id")).thenReturn(nodeId);
        when(mockResultSet.iterator()).thenReturn(Collections.singletonList(mockRow).iterator());

        // Mock the nodes list to contain the unavailable node
        Node mockNode = mock(Node.class);
        when(mockNode.getDatacenter()).thenReturn("datacenter");
        when(mockNode.getHostId()).thenReturn(nodeId);
        when(nativeConnectionProvider.getNodes()).thenReturn(Collections.singletonList(mockNode));

        // Simulate failed reconnects up to max attempts
        when(jmxConnectionProvider.getJmxConnector(nodeId)).thenReturn(null);

        // Call the method under test
        retrySchedulerService.retryNodes();

        // Verify that the node is marked as UNREACHABLE after reaching max retry attempts
        verify(eccNodesSync).updateNodeStatus(NodeStatus.UNREACHABLE, "datacenter", nodeId);
    }

    @Test
    void testDestroyShutdownService()
    {
        retrySchedulerService.destroy();
        assertTrue(retrySchedulerService.getMyScheduler().isShutdown());
    }
}
