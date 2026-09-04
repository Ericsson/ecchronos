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
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaRefresher;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages a pool of {@link NodeWorker} instances, one per Cassandra node.
 * Handles adding/removing nodes dynamically and broadcasting repair events to all workers.
 */
public class NodeWorkerManager
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeWorkerManager.class);
    private final Map<UUID, NodeWorker> myWorkers = new ConcurrentHashMap<>();
    private final ThreadPoolTaskExecutor myThreadPool;
    private final SchemaRefresher mySchemaRefresher;

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final Object myLock = new Object();

    /**
     * Constructs a NodeWorkerManager using the provided builder configuration.
     *
     * @param builder the builder containing configuration for the manager.
     */
    protected NodeWorkerManager(final Builder builder)
    {
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        mySchemaRefresher = builder.mySchemaRefresher;
        Collection<Node> nodes = myNativeConnectionProvider.getNodes().values();
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
     *
     * @param node the Cassandra node to create a worker for.
     */
    protected void addNewNodeToThreadPool(final Node node)
    {
        NodeWorker worker = new NodeWorker(node, mySchemaRefresher);
        LOG.info("New worker created for Node {}", node.getHostId());
        myWorkers.put(node.getHostId(), worker);
        int requiredPoolSize = myWorkers.size();
        if (myThreadPool.getMaxPoolSize() < requiredPoolSize)
        {
            LOG.info("Increasing thread pool max size from {} to {}",
                    myThreadPool.getMaxPoolSize(), requiredPoolSize);
            myThreadPool.setMaxPoolSize(requiredPoolSize);
        }
        if (myThreadPool.getCorePoolSize() < requiredPoolSize)
        {
            LOG.info("Increasing thread pool core size from {} to {}",
                    myThreadPool.getCorePoolSize(), requiredPoolSize);
            myThreadPool.setCorePoolSize(requiredPoolSize);
        }
        myThreadPool.submit(worker);
        Set<KeyspaceCreatedEvent> events = createKeyspacesEventsForNewNode();
        LOG.info("Created {} KeyspaceCreatedEvents for node {}", events.size(), node.getHostId());
        events.forEach(event ->
        {
            LOG.info("Submitting KeyspaceCreatedEvent for keyspace '{}' to worker of node {}",
                    event.keyspace().getName().asInternal(), node.getHostId());
            myWorkers.get(node.getHostId()).submitEvent(event);
        });
    }

    private Set<KeyspaceCreatedEvent> createKeyspacesEventsForNewNode()
    {
        Set<KeyspaceCreatedEvent> events = new HashSet<>();
        Collection<KeyspaceMetadata> keyspaces = myNativeConnectionProvider.getCqlSession().getMetadata().getKeyspaces().values();
        LOG.info("Found {} keyspaces in metadata for new node events", keyspaces.size());
        keyspaces.forEach(ks ->
        {
            LOG.info("Creating KeyspaceCreatedEvent for keyspace '{}'", ks.getName().asInternal());
            events.add(new KeyspaceCreatedEvent(ks));
        });
        return events;
    }

    /**
     * Adds a node to the worker pool if it is not already present.
     *
     * @param node the Cassandra node to add.
     */
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

    /**
     * Removes a node from the worker pool and deschedules all its jobs.
     *
     * @param node the Cassandra node to remove.
     */
    public final synchronized void removeNode(final Node node)
    {
        synchronized (myLock)
        {
            if (myWorkers.containsKey(node.getHostId()))
            {
                LOG.info("Removing node {} and descheduling all its jobs", node.getHostId());
                mySchemaRefresher.removeAllConfigurationsForNode(node.getHostId());
                NodeWorker nodeWorker = myWorkers.remove(node.getHostId());
                myThreadPool.stop(nodeWorker);
                int newSize = Math.max(1, myWorkers.size());
                myThreadPool.setCorePoolSize(newSize);
                myThreadPool.setMaxPoolSize(newSize);
            }
        }
    }

    /**
     * Broadcasts a repair event to all active node workers.
     *
     * @param event the repair event to broadcast.
     */
    public final void broadcastEvent(final RepairEvent event)
    {
        myWorkers.values().parallelStream()
                .forEach(nodeWorker -> nodeWorker.submitEvent(event));
    }

    /**
     * Shuts down the worker manager and its thread pool.
     */
    public final void shutdown()
    {
        myWorkers.clear();
        myThreadPool.shutdown();
    }

    /**
     * Gets the collection of active node workers. Visible for testing.
     *
     * @return the collection of node workers.
     */
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

    /**
     * Gets the map of node workers keyed by node UUID.
     *
     * @return the workers map.
     */
    public final Map<UUID, NodeWorker> getMyWorkers()
    {
        return myWorkers;
    }

    /**
     * Gets the thread pool task executor used by the manager.
     *
     * @return the thread pool task executor.
     */
    public final ThreadPoolTaskExecutor getMyThreadPool()
    {
        return myThreadPool;
    }

    /**
     * Gets the distributed native connection provider.
     *
     * @return the native connection provider.
     */
    public final DistributedNativeConnectionProvider getMyNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    /**
     * Builder for constructing {@link NodeWorkerManager} instances.
     */
    public static class Builder
    {
        private DistributedNativeConnectionProvider myNativeConnectionProvider;
        private ThreadPoolTaskExecutor myThreadPool;
        private SchemaRefresher mySchemaRefresher;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
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
         * Build with schema refresher.
         *
         * @param schemaRefresher the schema refresher.
         * @return Builder
         */
        public final Builder withSchemaRefresher(final SchemaRefresher schemaRefresher)
        {
            mySchemaRefresher = schemaRefresher;
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

