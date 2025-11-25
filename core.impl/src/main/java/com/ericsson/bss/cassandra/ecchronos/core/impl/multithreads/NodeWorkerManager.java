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
package com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class NodeWorkerManager
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeWorkerManager.class);
    private final Map<UUID, NodeWorker> myWorkers = new ConcurrentHashMap<>();
    private final ThreadPoolTaskExecutor myThreadPool;

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final RepairScheduler myRepairScheduler;
    private final TableReferenceFactory myTableReferenceFactory;
    private final Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
    private final Object myLock = new Object();

    protected NodeWorkerManager(final Builder builder)
    {
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        Collection<Node> nodes = myNativeConnectionProvider.getNodes().values();
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myRepairScheduler = builder.myRepairScheduler;
        myTableReferenceFactory = builder.myTableReferenceFactory;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;
        myThreadPool = builder.myThreadPool;
        myThreadPool.initialize();
        setupInitialNodeWorkers(nodes);
    }

    private void setupInitialNodeWorkers(final Collection<Node> nodes)
    {
        nodes.forEach(this::addNewNodeToThreadPool);
    }

    /**
     * Creates a NodeWorker and adds it to the ThreadPool.
     * @param node
     */
    protected void addNewNodeToThreadPool(final Node node)
    {
        NodeWorker worker = new NodeWorker(
                node,
                myReplicatedTableProvider,
                myRepairScheduler,
                Preconditions.checkNotNull(myTableReferenceFactory,
                        "Table reference factory must be set"),
                myRepairConfigurationFunction,
                myNativeConnectionProvider.getCqlSession());
        LOG.debug("New worker created for Node {}", node.getHostId());
        myWorkers.put(node.getHostId(), worker);
        myThreadPool.submit(worker);
    }

    public final synchronized void addNode(final Node node)
    {
        LOG.debug("addNode Node {}", node.getHostId());
        synchronized (myLock)
        {
            if (!myWorkers.containsKey(node.getHostId()))
            {
                LOG.debug("Node {} being added to the threadpool", node.getHostId());
                addNewNodeToThreadPool(node);
            }
            else
            {
                LOG.debug("Node {} is already in the workers", node.getHostId());
            }
        }
    }

    public final synchronized void removeNode(final Node node)
    {
        synchronized (myLock)
        {
            if (myWorkers.containsKey(node.getHostId()))
            {
                NodeWorker nodeWorker = myWorkers.get(node.getHostId());
                myWorkers.remove(node.getHostId());
                myThreadPool.stop(nodeWorker);
            }
        }
    }

    public final void broadcastEvent(final RepairEvent event)
    {
        myWorkers.values().parallelStream()
                .forEach(nodeWorker -> nodeWorker.submitEvent(event));
    }

    public final void shutdown()
    {
        myThreadPool.shutdown();
    }

    @VisibleForTesting
    public final Collection<NodeWorker> getWorkers()
    {
        return myWorkers.values();
    }

    /**
     * Create Builder for NodeWorkerManager.
     * @return Builder the Builder instance for the class.
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    public final Map<UUID, NodeWorker> getMyWorkers()
    {
        return myWorkers;
    }

    public final ThreadPoolTaskExecutor getMyThreadPool()
    {
        return myThreadPool;
    }

    public final DistributedNativeConnectionProvider getMyNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    public final ReplicatedTableProvider getMyReplicatedTableProvider()
    {
        return myReplicatedTableProvider;
    }

    public final RepairScheduler getMyRepairScheduler()
    {
        return myRepairScheduler;
    }

    public final TableReferenceFactory getMyTableReferenceFactory()
    {
        return myTableReferenceFactory;
    }

    public final Function<TableReference, Set<RepairConfiguration>> getMyRepairConfigurationFunction()
    {
        return myRepairConfigurationFunction;
    }

    public static class Builder
    {
        private DistributedNativeConnectionProvider myNativeConnectionProvider;
        private ReplicatedTableProvider myReplicatedTableProvider;
        private RepairScheduler myRepairScheduler;
        private TableReferenceFactory myTableReferenceFactory;
        private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
        private ThreadPoolTaskExecutor myThreadPool;

        /**
         * Build with repair configuration.
         *
         * @param defaultRepairConfiguration The default repair configuration
         * @return Builder
         */
        public Builder withRepairConfiguration(final Function<TableReference, Set<RepairConfiguration>>
                                                                                          defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = defaultRepairConfiguration;
            return this;
        }

        /**
         * Build with replicated table provider.
         *
         * @param replicatedTableProvider The replicated table provider
         * @return Builder
         */
        public Builder withReplicatedTableProvider(final ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        /**
         * Build with table repair scheduler.
         *
         * @param repairScheduler The repair scheduler
         * @return Builder
         */
        public Builder withRepairScheduler(final RepairScheduler repairScheduler)
        {
            myRepairScheduler = repairScheduler;
            return this;
        }

        /**
         * Build with run DistributedNativeConnectionProvider.
         *
         * @param nativeConnection the Native Connection that contains Cassandra nodes.
         * @return Builder Native Connection
         */
        public Builder withNativeConnection(final DistributedNativeConnectionProvider nativeConnection)
        {
            myNativeConnectionProvider = nativeConnection;
            return this;
        }

        /**
         * Build with table reference factory.
         *
         * @param tableReferenceFactory The table reference factory
         * @return Builder
         */
        public Builder withTableReferenceFactory(final TableReferenceFactory tableReferenceFactory)
        {
            myTableReferenceFactory = tableReferenceFactory;
            return this;
        }

        /**
         * Build with thread pool task executor.
         *
         * @param threadPool The thread pool task executor.
         * @return Builder
         */
        public Builder withThreadPool(final ThreadPoolTaskExecutor threadPool)
        {
            myThreadPool = threadPool;
            return this;
        }

        /**
         * Build.
         *
         * @return DefaultRepairConfigurationProvider
         */
        public NodeWorkerManager build()
        {
            return new NodeWorkerManager(this);
        }
    }
}

