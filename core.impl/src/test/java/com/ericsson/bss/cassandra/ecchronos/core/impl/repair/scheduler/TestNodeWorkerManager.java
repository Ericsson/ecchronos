/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableDroppedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeWorkerManager
{
    @Mock
    private DistributedNativeConnectionProvider nativeConnectionProvider;

    @Mock
    private ReplicatedTableProvider replicatedTableProvider;

    @Mock
    private RepairScheduler repairScheduler;

    @Mock
    private TableReferenceFactory tableReferenceFactory;

    private NoopThreadPoolTaskExecutor threadPool;

    @Mock
    private Function<TableReference, Set<RepairConfiguration>> repairConfigFunction;

    @Mock
    private Node node1;

    @Mock
    private Node node2;

    @Mock
    private Node node3;

    @Mock
    private Node node4;

    @Mock
    private CqlSession cqlSession;

    @Mock
    private KeyspaceMetadata keyspaceMetadata;

    @Mock
    private TableMetadata tableMetadata;

    private final UUID node1UUID = UUID.randomUUID();
    private final UUID node2UUID = UUID.randomUUID();
    private NodeWorkerManager manager;

    @Before
    public void setup()
    {
        Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(node1UUID, node1);
        nodes.put(node2UUID, node2);
        threadPool = new NoopThreadPoolTaskExecutor();

        when(nativeConnectionProvider.getCqlSession()).thenReturn(cqlSession);
        when(nativeConnectionProvider.getNodes()).thenReturn(nodes);
        when(nativeConnectionProvider.getNodes()).thenReturn(nodes);

        manager = NodeWorkerManager.newBuilder()
                .withNativeConnection(nativeConnectionProvider)
                .withReplicatedTableProvider(replicatedTableProvider)
                .withRepairScheduler(repairScheduler)
                .withTableReferenceFactory(tableReferenceFactory)
                .withRepairConfiguration(repairConfigFunction)
                .withThreadPool(threadPool)
                .build();
    }

    @Test
    public void testInitialWorkerSetup()
    {
        assertEquals(2, threadPool.getSubmittedTasks().size());
    }

    @Test
    public void testAddNode()
    {
        assertEquals(2, threadPool.getSubmittedTasks().size());

        manager.addNode(node3);
        assertEquals(3, threadPool.getSubmittedTasks().size());

        manager.addNode(node4);
        assertEquals(4, threadPool.getSubmittedTasks().size());
    }

    @Test
    public void testRemoveNode()
    {
        assertEquals(2, threadPool.getSubmittedTasks().size());

        manager.removeNode(node2);
        assertEquals(1, threadPool.getSubmittedTasks().size());

        manager.removeNode(node1);
        assertEquals(0, threadPool.getSubmittedTasks().size());
    }

    @Test
    public void testWorkersTasks()
    {
        assertEquals(2, threadPool.getSubmittedTasks().size());
        KeyspaceCreatedEvent keyspaceCreatedEvent = new KeyspaceCreatedEvent(keyspaceMetadata);
        TableCreatedEvent tableCreatedEvent = new TableCreatedEvent(tableMetadata);
        TableDroppedEvent tableDroppedEvent = new TableDroppedEvent(tableMetadata);

        manager.broadcastEvent(keyspaceCreatedEvent);
        manager.broadcastEvent(tableCreatedEvent);
        manager.broadcastEvent(tableDroppedEvent);

        for (NodeWorker nodeWorker : manager.getWorkers())
        {
            assertEquals(3, nodeWorker.getQueueSize());
        }

    }

    public static class NoopThreadPoolTaskExecutor extends ThreadPoolTaskExecutor
    {

        private final List<Runnable> submittedTasks = new ArrayList<>();

        @Override
        public void initialize()
        {
            // No-op
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            submittedTasks.add(task);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void shutdown() {
            // No-op
        }

        @Override
        public void stop(Runnable task)
        {
            submittedTasks.remove(task);
        }

        public List<Runnable> getSubmittedTasks()
        {
            return submittedTasks;
        }
    }

}
