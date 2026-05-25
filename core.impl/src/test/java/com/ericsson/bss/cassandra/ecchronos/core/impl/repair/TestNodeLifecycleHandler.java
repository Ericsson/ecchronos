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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeLifecycleHandler
{
    @Mock
    private EccNodesSync mockEccNodesSync;
    @Mock
    private DistributedJmxConnectionProvider mockJmxProvider;
    @Mock
    private DistributedNativeConnectionProvider mockNativeProvider;
    @Mock
    private NodeWorkerManager mockWorkerManager;
    @Mock
    private ScheduleManager mockScheduleManager;
    @Mock
    private ExecutorService mockExecutor;
    @Mock
    private Node mockNode;

    private final UUID nodeId = UUID.randomUUID();
    private NodeLifecycleHandler handler;

    @Before
    public void setup()
    {
        when(mockNode.getHostId()).thenReturn(nodeId);
        handler = new NodeLifecycleHandler(mockEccNodesSync, mockJmxProvider,
                mockNativeProvider, mockWorkerManager, mockScheduleManager, mockExecutor);
    }

    @Test
    public void testOnAddValidNodeSubmitsTaskAndAddsToWorker()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);

        handler.onAdd(mockNode);

        verify(mockExecutor).submit(any(Callable.class));
        verify(mockWorkerManager).addNode(mockNode);
        verify(mockScheduleManager).createScheduleFutureForNode(nodeId);
    }

    @Test
    public void testOnAddInvalidNodeDoesNothing()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(false);

        handler.onAdd(mockNode);

        verify(mockExecutor, never()).submit(any(Callable.class));
        verify(mockWorkerManager, never()).addNode(any());
    }

    @Test
    public void testOnRemoveValidNodeSubmitsTaskAndRemovesFromWorker()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);

        handler.onRemove(mockNode);

        verify(mockExecutor).submit(any(Callable.class));
        verify(mockWorkerManager).removeNode(mockNode);
        verify(mockScheduleManager).removeScheduleFutureForNode(nodeId);
    }

    @Test
    public void testOnRemoveInvalidNodeDoesNothing()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(false);

        handler.onRemove(mockNode);

        verify(mockExecutor, never()).submit(any(Callable.class));
        verify(mockWorkerManager, never()).removeNode(any());
    }

    @Test
    public void testOnUpExistingNodeRefreshesConnection()
    {
        Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(nodeId, mockNode);
        when(mockNativeProvider.getNodes()).thenReturn(nodes);

        handler.onUp(mockNode);

        verify(mockExecutor).submit(any(Callable.class));
        verify(mockScheduleManager).createScheduleFutureForNode(nodeId);
        verify(mockWorkerManager, never()).addNode(any());
    }

    @Test
    public void testOnUpUnknownNodeDelegatesToOnAdd()
    {
        when(mockNativeProvider.getNodes()).thenReturn(new HashMap<>());
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);

        handler.onUp(mockNode);

        verify(mockWorkerManager).addNode(mockNode);
        verify(mockScheduleManager).createScheduleFutureForNode(nodeId);
    }

    @Test
    public void testOnAddClearsTokenRangeCache()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);
        LongTokenRange.clearCache();
        LongTokenRange.of(1, 100);
        LongTokenRange.of(2, 200);
        assertThat(LongTokenRange.cacheSize()).isEqualTo(2);

        handler.onAdd(mockNode);

        assertThat(LongTokenRange.cacheSize()).isEqualTo(0);
    }

    @Test
    public void testOnRemoveClearsTokenRangeCache()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);
        LongTokenRange.clearCache();
        LongTokenRange.of(1, 100);
        LongTokenRange.of(2, 200);
        assertThat(LongTokenRange.cacheSize()).isEqualTo(2);

        handler.onRemove(mockNode);

        assertThat(LongTokenRange.cacheSize()).isEqualTo(0);
    }
}
