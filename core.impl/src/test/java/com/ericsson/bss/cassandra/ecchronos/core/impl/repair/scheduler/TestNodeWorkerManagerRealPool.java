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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaRefresher;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Regression tests for issue #1992 using a <em>real</em> {@link ThreadPoolTaskExecutor} (the no-op test double in
 * {@link TestNodeWorkerManager} hides the Spring pause behaviour). Removing a node must neither pause the pool nor
 * leave a zombie worker occupying a slot, so nodes added afterwards still get a running worker.
 */
public class TestNodeWorkerManagerRealPool
{
    private static final int WAIT_SECONDS = 10;

    private SchemaRefresher mySchemaRefresher;
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    private ThreadPoolTaskExecutor myThreadPool;
    private NodeWorkerManager myManager;
    private final Map<UUID, Node> myNodes = new HashMap<>();

    @Before
    public void setup()
    {
        mySchemaRefresher = mock(SchemaRefresher.class);
        myNativeConnectionProvider = mock(DistributedNativeConnectionProvider.class);

        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getName()).thenReturn(CqlIdentifier.fromInternal("ks"));
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspaces()).thenReturn(Map.of(CqlIdentifier.fromInternal("ks"), keyspace));
        CqlSession session = mock(CqlSession.class);
        when(session.getMetadata()).thenReturn(metadata);
        when(myNativeConnectionProvider.getCqlSession()).thenReturn(session);
        when(myNativeConnectionProvider.getNodes()).thenReturn(myNodes);

        // Same shape as ECChronos.setupThreadPool() with the default thread pool configuration.
        myThreadPool = new ThreadPoolTaskExecutor();
        myThreadPool.setCorePoolSize(4);
        myThreadPool.setMaxPoolSize(10);
        myThreadPool.setQueueCapacity(20);
        myThreadPool.setKeepAliveSeconds(60);
        myThreadPool.setThreadNamePrefix("NodeWorker-");
        myThreadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        myNodes.put(newNodeId(), newNode());
        myNodes.put(newNodeId(), newNode());
        myNodes.put(newNodeId(), newNode());
    }

    @After
    public void tearDown()
    {
        if (myManager != null)
        {
            myManager.shutdown();
        }
    }

    @Test
    public void testNodeAddedAfterRemovalGetsRunningWorker()
    {
        myManager = buildManager();
        Node removed = anyInitialNode();

        myManager.removeNode(removed);
        assertThat(myThreadPool.isRunning()).isTrue();

        Node added = newNode();
        myManager.addNode(added);

        verify(mySchemaRefresher, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)))
                .onKeyspaceCreated(eq(added), any(KeyspaceCreatedEvent.class));
        assertThat(myThreadPool.isRunning()).isTrue();
    }

    @Test
    public void testSeveralRemovalsFollowedBySeveralAdditionsLeaveAllWorkersRunning()
    {
        myManager = buildManager();
        Node[] initial = myNodes.values().toArray(new Node[0]);

        myManager.removeNode(initial[0]);
        myManager.removeNode(initial[1]);

        Node added1 = newNode();
        Node added2 = newNode();
        Node added3 = newNode();
        myManager.addNode(added1);
        myManager.addNode(added2);
        myManager.addNode(added3);

        for (Node added : new Node[] {added1, added2, added3})
        {
            verify(mySchemaRefresher, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)))
                    .onKeyspaceCreated(eq(added), any(KeyspaceCreatedEvent.class));
        }
        // Nothing may be left stuck behind a pool slot, and the surviving worker still works.
        await().atMost(WAIT_SECONDS, TimeUnit.SECONDS)
                .until(() -> myThreadPool.getThreadPoolExecutor().getQueue().isEmpty());
        assertThat(myThreadPool.isRunning()).isTrue();
        assertThat(myManager.getWorkers()).hasSize(4);
    }

    @Test
    public void testNodeRemovedAndReAddedByReconciliationGetsRunningWorker()
    {
        myManager = buildManager();
        Node node = anyInitialNode();

        // The daily reconciliation (ReloadSchedulerService.compareNodesMap) can remove and re-add the same node.
        myManager.removeNode(node);
        myManager.addNode(node);

        // Initial setup already delivered one event for this node; the re-added worker must deliver a second.
        verify(mySchemaRefresher, timeout(TimeUnit.SECONDS.toMillis(WAIT_SECONDS)).times(2))
                .onKeyspaceCreated(eq(node), any(KeyspaceCreatedEvent.class));
        assertThat(myThreadPool.isRunning()).isTrue();
    }

    @Test
    public void testRemovingWorkerFreesItsThread()
    {
        myManager = buildManager();
        // All three initial workers are in flight, each blocked waiting for events.
        await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).until(() -> myThreadPool.getActiveCount() == 3);

        myManager.removeNode(anyInitialNode());

        // The removed worker's thread is interrupted and returns to the pool instead of lingering as a zombie.
        await().atMost(WAIT_SECONDS, TimeUnit.SECONDS).until(() -> myThreadPool.getActiveCount() == 2);
    }

    private NodeWorkerManager buildManager()
    {
        return NodeWorkerManager.newBuilder()
                .withNativeConnection(myNativeConnectionProvider)
                .withSchemaRefresher(mySchemaRefresher)
                .withThreadPool(myThreadPool)
                .build();
    }

    private Node anyInitialNode()
    {
        return myNodes.values().iterator().next();
    }

    private static UUID newNodeId()
    {
        return UUID.randomUUID();
    }

    private Node newNode()
    {
        UUID hostId = newNodeId();
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(hostId);
        return node;
    }
}
