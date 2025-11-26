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

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeAddedAction;
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeRemovedAction;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.CloseEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.SetupEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableDroppedEvent;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A repair configuration provider that adds configuration to {@link NodeWorkerManager} based on whether the table
 * is replicated locally using the default repair configuration provided during construction of this object.
 */
public class DefaultRepairConfigurationProvider extends NodeStateListenerBase implements SchemaChangeListener
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRepairConfigurationProvider.class);
    private static final Integer NO_OF_THREADS = 1;
    private NodeWorkerManager myWorkerManager;

    private CqlSession mySession;
    private final ExecutorService myService;
    private EccNodesSync myEccNodesSync;
    private DistributedJmxConnectionProvider myJmxConnectionProvider;
    private DistributedNativeConnectionProvider myAgentNativeConnectionProvider;
    private ScheduleManager myScheduleManager;

    /**
     * Default constructor.
     */
    public DefaultRepairConfigurationProvider()
    {
        myService = Executors.newFixedThreadPool(NO_OF_THREADS);
    }

    private DefaultRepairConfigurationProvider(final Builder builder)
    {
        mySession = builder.mySession;
        myEccNodesSync = builder.myEccNodesSync;
        myJmxConnectionProvider = builder.myJmxConnectionProvider;
        myAgentNativeConnectionProvider = builder.myAgentNativeConnectionProvider;
        setupConfiguration();
        myService = Executors.newFixedThreadPool(NO_OF_THREADS);
        myScheduleManager = builder.myScheduleManager;

    }

    /**
     * From builder.
     *
     * @param builder A builder
     */
    public void fromBuilder(final Builder builder)
    {
        mySession = builder.mySession;
        myEccNodesSync = builder.myEccNodesSync;
        myJmxConnectionProvider = builder.myJmxConnectionProvider;
        myAgentNativeConnectionProvider = builder.myAgentNativeConnectionProvider;
        myWorkerManager = builder.myNodeWorkerManager;
        myScheduleManager = builder.myScheduleManager;
        setupConfiguration();
    }

    /**
     * Called when keyspace is created.
     *
     * @param keyspace Keyspace metadata
     */
    @Override
    public void onKeyspaceCreated(final KeyspaceMetadata keyspace)
    {
        myWorkerManager.broadcastEvent(new KeyspaceCreatedEvent(keyspace));
    }

    /**
     * Called when keyspace is updated.
     *
     * @param current Current keyspace metadata
     * @param previous Previous keyspace metadata
     */
    @Override
    public void onKeyspaceUpdated(final KeyspaceMetadata current,
            final KeyspaceMetadata previous)
    {
        onKeyspaceCreated(current);
    }

    /**
     * Called when keyspace is dropped.
     *
     * @param keyspace Keyspace metadata
     */
    @Override
    public void onKeyspaceDropped(final KeyspaceMetadata keyspace)
    {
        keyspace.getTables().values().forEach(this::onTableDropped);
    }

    /**
     * Called when table is created.
     *
     * @param table Table metadata
     */
    @Override
    public void onTableCreated(final TableMetadata table)
    {
        myWorkerManager.broadcastEvent(new TableCreatedEvent(table));
    }

    /**
     * Called when table is dropped.
     *
     * @param table Table metadata
     */
    @Override
    public void onTableDropped(final TableMetadata table)
    {
        myWorkerManager.broadcastEvent(new TableDroppedEvent(table));
    }

    /**
     * Called when table is updated.
     *
     * @param current Current table metadata
     * @param previous Previous table metadata
     */
    @Override
    public void onTableUpdated(final TableMetadata current, final TableMetadata previous)
    {
        onTableCreated(current);
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        if (mySession != null)
        {
            for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
            {
                myWorkerManager.broadcastEvent(new CloseEvent(keyspaceMetadata));
            }
        }
    }

    /**
     * Create Builder for DefaultRepairConfigurationProvider.
     * @return Builder the Builder instance for the class.
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * Called when user defined types are created.
     *
     * @param type User defined type
     */
    @Override
    public void onUserDefinedTypeCreated(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * Called when user defined types are dropped.
     *
     * @param type User defined type
     */
    @Override
    public void onUserDefinedTypeDropped(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * Called when user defined types are updated.
     *
     * @param current Current user defined type
     * @param previous previous user defined type
     */
    @Override
    public void onUserDefinedTypeUpdated(final UserDefinedType current, final UserDefinedType previous)
    {
        // NOOP
    }

    /**
     * Called when functions are created.
     *
     * @param function Function metadata
     */
    @Override
    public void onFunctionCreated(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * Called when functions are dropped.
     *
     * @param function Function metadata
     */
    @Override
    public void onFunctionDropped(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * Called when functions are updated.
     *
     * @param current Current function metadata
     * @param previous Previous function metadata
     */
    @Override
    public void onFunctionUpdated(final FunctionMetadata current, final FunctionMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when aggregates are created.
     *
     * @param aggregate Aggregate metadata
     */
    @Override
    public void onAggregateCreated(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * Called when aggregates are dropped.
     *
     * @param aggregate Aggregate metadata
     */
    @Override
    public void onAggregateDropped(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * Called when aggregates are updated.
     *
     * @param current Current aggregate metadata
     * @param previous previous aggregate metadata
     */
    @Override
    public void onAggregateUpdated(final AggregateMetadata current, final AggregateMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when views are created.
     *
     * @param view View metadata
     */
    @Override
    public void onViewCreated(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * Called when views are dropped.
     *
     * @param view View metadata
     */
    @Override
    public void onViewDropped(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * Called when views are updated.
     *
     * @param current Current view metadata
     * @param previous Previous view metadata
     */
    @Override
    public void onViewUpdated(final ViewMetadata current, final ViewMetadata previous)
    {
        // NOOP
    }

    /**
     * Called when the session is up and ready. Will invoke the listeners' onSessionReady methods.
     *
     * @param session The session
     */
    @Override
    public void onSessionReady(final Session session)
    {
        SchemaChangeListener.super.onSessionReady(session);
    }

    /**
     * Callback for when a node switches state to UP.
     *
     * @param node The node switching state to UP
     */
    @Override
    public void onUp(final Node node)
    {
        LOG.debug("{} switched state to UP.", node);
        if (myAgentNativeConnectionProvider == null  || !myAgentNativeConnectionProvider.getNodes().containsKey(node.getHostId()))
        {
            onAdd(node);
        }
        else
        {
            if (myScheduleManager != null)
            {
                myScheduleManager.createScheduleFutureForNode(node.getHostId());
            }
            else
            {
                LOG.debug("myScheduleManager not ready when Node added {}", node.getHostId());
            }
        }

        setupConfiguration();
    }

    /**
     * Callback for when a node switches state to DOWN.
     *
     * @param node The node switching state to DOWN
     */
    @Override
    public void onDown(final Node node)
    {
        LOG.debug("{} switched state to DOWN.", node);
        setupConfiguration();
    }

    /**
     * Callback for when a new node is added to the cluster.
     * @param node the node to add.
     */
    @Override
    public void onAdd(final Node node)
    {
        if (myAgentNativeConnectionProvider == null  || myAgentNativeConnectionProvider.confirmNodeValid(node))
        {
            LOG.info("Node added {}", node.getHostId());

            NodeAddedAction callable = new NodeAddedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
            myService.submit(callable);
            if (myWorkerManager != null)
            {
                myWorkerManager.addNode(node);
            }
            if (myScheduleManager != null)
            {
                myScheduleManager.createScheduleFutureForNode(node.getHostId());
            }
            else
            {
                LOG.debug("myScheduleManager not ready when Node added {}", node.getHostId());
            }
        }
    }

    /**
     * callback for when a node is removed from the cluster.
     * @param node the node to remove.
     */
    @Override
    public void onRemove(final Node node)
    {
        if (myAgentNativeConnectionProvider == null  || myAgentNativeConnectionProvider.confirmNodeValid(node))
        {
            LOG.info("Node removed {}", node.getHostId());
            NodeRemovedAction callable = new NodeRemovedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
            myService.submit(callable);
            if (myWorkerManager != null)
            {
                myWorkerManager.removeNode(node);
            }
            if (myScheduleManager != null)
            {
                myScheduleManager.removeScheduleFutureForNode(node.getHostId());
            }

        }
    }

    /**
     * This will go through all the configuration, given mySession is set, otherwise it will just silently
     * return.
     */
    private void setupConfiguration()
    {
        if (mySession == null)
        {
            LOG.debug("Session during setupConfiguration call was null.");
            return;
        }

        for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
        {
            myWorkerManager.broadcastEvent(new SetupEvent(keyspaceMetadata));

        }
    }

    /**
     * Builder for DefaultRepairConfigurationProvider.
     */
    public static class Builder
    {
        private CqlSession mySession;
        private EccNodesSync myEccNodesSync;
        private DistributedJmxConnectionProvider myJmxConnectionProvider;
        private DistributedNativeConnectionProvider myAgentNativeConnectionProvider;
        private NodeWorkerManager myNodeWorkerManager;
        private ScheduleManager myScheduleManager;


        /**
         * Build with session.
         *
         * @param session The CQl session
         * @return Builder
         */
        public Builder withSession(final CqlSession session)
        {
            mySession = session;
            return this;
        }

        /**
         * Build with EccNodesSync.
         * @param eccNodesSync the base table to store node info.
         * @return Builder with EccNodesSync
         */
        public Builder withEccNodesSync(final EccNodesSync eccNodesSync)
        {
            myEccNodesSync = eccNodesSync;
            return this;
        }

        /**
         * Build with DistributedNativeConnectionProvider.
         * @param agentNativeConnectionProvider the native connection.
         * @return Builder
         */
        public Builder withDistributedNativeConnectionProvider(final DistributedNativeConnectionProvider agentNativeConnectionProvider)
        {
            myAgentNativeConnectionProvider = agentNativeConnectionProvider;
            return this;
        }

        /**
         * Build with DistributedJmxConnectionProvider.
         * @param jmxConnectionProvider the jmx connection.
         * @return Builder with DistributedJmxConnectionProvider
         */
        public Builder withJmxConnectionProvider(final DistributedJmxConnectionProvider jmxConnectionProvider)
        {
            myJmxConnectionProvider = jmxConnectionProvider;
            return this;
        }

        /**
         * Build with NodeWorkerManager.
         * @param nodeWorkerManager the node worker manager.
         * @return Builder with NodeWorkerManager
         */
        public Builder withNodeWorkerManager(final NodeWorkerManager nodeWorkerManager)
        {
            myNodeWorkerManager = nodeWorkerManager;
            return this;
        }
        /**
         * Build with scheduleManager.
         * @param scheduleManager the schedule Manager
         * @return Builder with EccNodesSync
         */
        public Builder withScheduleManager(final ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        /**
         * Build.
         *
         * @return DefaultRepairConfigurationProvider
         */
        public DefaultRepairConfigurationProvider build()
        {
            return new DefaultRepairConfigurationProvider(this);
        }
    }
}

