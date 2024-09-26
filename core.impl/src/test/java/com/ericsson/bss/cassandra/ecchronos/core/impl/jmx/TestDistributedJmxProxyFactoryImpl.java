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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import com.datastax.oss.driver.api.core.metadata.Node;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import java.util.*;
import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestDistributedJmxProxyFactoryImpl
{

    @Mock
    private DistributedJmxConnectionProvider mockConnectionProvider;

    @Mock
    private JMXConnector mockConnector;

    @Mock
    private MBeanServerConnection mockMBeanServerConnection;

    @Mock
    private EccNodesSync mockEccNodesSync;

    private DistributedJmxProxy distributedJmxProxy;

    private final  UUID nodeId = UUID.randomUUID();
    private final Map<UUID, Node> mockNodesMap = new HashMap<>();

    @Before
    public void setUp() throws Exception
    {
        mockNodesMap.put(nodeId, mock(Node.class));

        when(mockConnectionProvider.getJmxConnector(nodeId)).thenReturn(mockConnector);
        when(mockConnector.getMBeanServerConnection()).thenReturn(mockMBeanServerConnection);

        distributedJmxProxy = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(mockConnectionProvider)
                .withNodesMap(mockNodesMap)
                .withEccNodesSync(mockEccNodesSync).build().connect();

        when(distributedJmxProxy.validateJmxConnection(mockConnector)).thenReturn(true);
    }

    @Test
    public void testGetLiveNodes() throws Exception
    {
        List<String> expectedLiveNodes = Arrays.asList("127.0.0.1", "192.168.0.1");

        when(mockMBeanServerConnection.getAttribute(any(ObjectName.class), any(String.class)))
                .thenReturn(expectedLiveNodes);

        List<String> liveNodes = distributedJmxProxy.getLiveNodes(nodeId);

        assertEquals(expectedLiveNodes, liveNodes);
        verify(mockMBeanServerConnection).getAttribute(any(ObjectName.class), eq("LiveNodes"));
    }

    @Test
    public void testGetUnreachableNodes() throws Exception
    {
        List<String> expectedUnreachableNodes = Arrays.asList("10.0.0.1", "10.0.0.2");

        when(mockMBeanServerConnection.getAttribute(any(ObjectName.class), eq("UnreachableNodes")))
                .thenReturn(expectedUnreachableNodes);

        List<String> unreachableNodes = distributedJmxProxy.getUnreachableNodes(nodeId);

        assertEquals(expectedUnreachableNodes, unreachableNodes);
        verify(mockMBeanServerConnection).getAttribute(any(ObjectName.class), eq("UnreachableNodes"));
    }

    @Test
    public void testRepairAsync() throws Exception
    {
        String keyspace = "test_keyspace";
        Map<String, String> options = new HashMap<>();
        options.put("option1", "value1");

        int expectedRepairId = 42;

        when(mockMBeanServerConnection.invoke(any(ObjectName.class), eq("repairAsync"),
                any(Object[].class), any(String[].class))).thenReturn(expectedRepairId);

        int repairId = distributedJmxProxy.repairAsync(nodeId, keyspace, options);

        assertEquals(expectedRepairId, repairId);
        verify(mockMBeanServerConnection).invoke(any(ObjectName.class), eq("repairAsync"),
                any(Object[].class), any(String[].class));
    }

    @Test
    public void testForceTerminateAllRepairSessionsInSpecificNode() throws Exception
    {

        distributedJmxProxy.forceTerminateAllRepairSessionsInSpecificNode(nodeId);

        verify(mockMBeanServerConnection).invoke(any(ObjectName.class), eq("forceTerminateAllRepairSessions"),
                isNull(), isNull());
    }

    @Test
    public void testLiveDiskSpaceUsed() throws Exception
    {
        TableReference mockTableReference = mock(TableReference.class);
        when(mockTableReference.getKeyspace()).thenReturn("test_keyspace");
        when(mockTableReference.getTable()).thenReturn("test_table");
        long expectedDiskSpaceUsed = 1024L;

        when(mockMBeanServerConnection.getAttribute(any(ObjectName.class), eq("Count"))).thenReturn(expectedDiskSpaceUsed);

        long diskSpaceUsed = distributedJmxProxy.liveDiskSpaceUsed(nodeId, mockTableReference);

        assertEquals(expectedDiskSpaceUsed, diskSpaceUsed);
        verify(mockMBeanServerConnection).getAttribute(any(ObjectName.class), eq("Count"));
    }

    @Test
    public void testGetMaxRepairedAt() throws Exception
    {
        TableReference mockTableReference = mock(TableReference.class);
        when(mockTableReference.getKeyspace()).thenReturn("test_keyspace");
        when(mockTableReference.getTable()).thenReturn("test_table");

        // Mocking the return of MBeanServerConnection.invoke() to return a list with CompositeData
        List<CompositeData> mockCompositeDataList = new ArrayList<>();
        CompositeData mockCompositeData = mock(CompositeData.class);

        // Adding the CompositeData mock object to the list
        mockCompositeDataList.add(mockCompositeData);

        // Configuring the behavior for getAll() on CompositeData
        when(mockCompositeData.getAll(any(String[].class))).thenReturn(new Object[] { 123456L });

        // Mocking the invoke method to return the mockCompositeDataList
        when(mockMBeanServerConnection.invoke(any(ObjectName.class), eq("getRepairStats"),
                any(Object[].class), any(String[].class))).thenReturn(mockCompositeDataList);

        // Now calling the method under test
        long maxRepairedAt = distributedJmxProxy.getMaxRepairedAt(nodeId, mockTableReference);

        // Assert that the value returned matches the mocked value
        assertEquals(123456L, maxRepairedAt);

        // Verifying interactions with the mock objects
        verify(mockMBeanServerConnection).invoke(any(ObjectName.class), eq("getRepairStats"),
                any(Object[].class), any(String[].class));
    }
}
