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
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.ReplicaSetCache;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
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
    public void testOnAddRegistersWorkerBeforeCreatingSchedule()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);

        handler.onAdd(mockNode);

        InOrder inOrder = inOrder(mockWorkerManager, mockScheduleManager);

        inOrder.verify(mockWorkerManager).addNode(mockNode);
        inOrder.verify(mockScheduleManager).createScheduleFutureForNode(nodeId);
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

    @Test
    public void testOnAddClearsReplicaSetCache()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);
        ReplicaSetCache cache = new ReplicaSetCache();
        cache.intern(ImmutableSet.of());
        assertThat(cache.size()).isEqualTo(1);

        NodeLifecycleHandler handlerWithCache = new NodeLifecycleHandler(mockEccNodesSync, mockJmxProvider,
                mockNativeProvider, mockWorkerManager, mockScheduleManager, mockExecutor, cache);

        handlerWithCache.onAdd(mockNode);

        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    public void testOnRemoveClearsReplicaSetCache()
    {
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);
        ReplicaSetCache cache = new ReplicaSetCache();
        cache.intern(ImmutableSet.of());
        assertThat(cache.size()).isEqualTo(1);

        NodeLifecycleHandler handlerWithCache = new NodeLifecycleHandler(mockEccNodesSync, mockJmxProvider,
                mockNativeProvider, mockWorkerManager, mockScheduleManager, mockExecutor, cache);

        handlerWithCache.onRemove(mockNode);

        assertThat(cache.size()).isEqualTo(0);
    }

    /**
     * Reproduces the race condition described in issue #1754.
     * <p>
     * When a node is added, the schedule for that node is created by
     * {@link ScheduleManager#createScheduleFutureForNode(UUID)}, whose run task immediately reads the node from
     * {@link DistributedNativeConnectionProvider#getNodes()}. If the node is only registered into the native
     * connection provider by the asynchronously submitted {@link NodeAddedAction}, then at the moment the schedule
     * is created the node is not yet present, the captured node is {@code null}, job validation rejects the jobs,
     * and no schedules are created until a restart.
     * <p>
     * This test uses a map-backed native provider and a mock executor that captures (but does not run) the submitted
     * action, mirroring the production window where the async action has not executed yet. It asserts the ordering
     * contract: the node must already be registered in the native provider when the schedule is created.
     */
    @Test
    public void testOnAddRegistersNodeInNativeProviderBeforeCreatingSchedule()
    {
        Map<UUID, Node> liveNodes = new HashMap<>();
        when(mockNativeProvider.confirmNodeValid(mockNode)).thenReturn(true);
        when(mockNativeProvider.getNodes()).thenReturn(liveNodes);
        // Model production: the native provider map is only populated by addNode (what NodeAddedAction calls).
        doAnswer(invocation ->
        {
            liveNodes.put(nodeId, mockNode);
            return null;
        }).when(mockNativeProvider).addNode(mockNode);

        AtomicBoolean nodePresentWhenScheduleCreated = new AtomicBoolean(false);
        doAnswer(invocation ->
        {
            // Assert via the provider, mirroring what ScheduleManagerImpl's JobRunTask reads when the
            // schedule is created: myNativeConnectionProvider.getNodes().get(nodeID).
            nodePresentWhenScheduleCreated.set(mockNativeProvider.getNodes().containsKey(nodeId));
            return null;
        }).when(mockScheduleManager).createScheduleFutureForNode(nodeId);

        handler.onAdd(mockNode);

        assertThat(nodePresentWhenScheduleCreated.get())
                .as("node must be registered in the native connection provider before its schedule is created")
                .isTrue();
    }

    /**
     * Same race as {@link #testOnAddRegistersNodeInNativeProviderBeforeCreatingSchedule()} but for the
     * {@code onUp} path of a node that is already known to the native provider. The refreshed schedule must
     * still find the node registered when it is (re)created.
     */
    @Test
    public void testOnUpRegistersNodeInNativeProviderBeforeCreatingSchedule()
    {
        Map<UUID, Node> liveNodes = new HashMap<>();
        liveNodes.put(nodeId, mockNode);
        when(mockNativeProvider.getNodes()).thenReturn(liveNodes);

        AtomicBoolean nodePresentWhenScheduleCreated = new AtomicBoolean(false);
        doAnswer(invocation ->
        {
            nodePresentWhenScheduleCreated.set(liveNodes.containsKey(nodeId));
            return null;
        }).when(mockScheduleManager).createScheduleFutureForNode(nodeId);

        handler.onUp(mockNode);

        assertThat(nodePresentWhenScheduleCreated.get())
                .as("node must be registered in the native connection provider before its schedule is created")
                .isTrue();
    }
}
