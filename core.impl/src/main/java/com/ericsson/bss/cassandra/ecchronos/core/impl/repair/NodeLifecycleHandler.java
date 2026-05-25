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
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeAddedAction;
import com.ericsson.bss.cassandra.ecchronos.core.impl.refresh.NodeRemovedAction;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.ReplicaSetCache;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Handles node lifecycle events (up, down, add, remove) by coordinating
 * JMX connections, node sync, worker management, and schedule creation.
 */
public class NodeLifecycleHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeLifecycleHandler.class);

    private final EccNodesSync myEccNodesSync;
    private final DistributedJmxConnectionProvider myJmxConnectionProvider;
    private final DistributedNativeConnectionProvider myAgentNativeConnectionProvider;
    private final NodeWorkerManager myWorkerManager;
    private final ScheduleManager myScheduleManager;
    private final ExecutorService myService;
    private final ReplicaSetCache myReplicaSetCache;

    public NodeLifecycleHandler(final EccNodesSync eccNodesSync,
                                final DistributedJmxConnectionProvider jmxConnectionProvider,
                                final DistributedNativeConnectionProvider agentNativeConnectionProvider,
                                final NodeWorkerManager workerManager,
                                final ScheduleManager scheduleManager,
                                final ExecutorService service)
    {
        this(eccNodesSync, jmxConnectionProvider, agentNativeConnectionProvider,
                workerManager, scheduleManager, service, null);
    }

    public NodeLifecycleHandler(final EccNodesSync eccNodesSync,
                                final DistributedJmxConnectionProvider jmxConnectionProvider,
                                final DistributedNativeConnectionProvider agentNativeConnectionProvider,
                                final NodeWorkerManager workerManager,
                                final ScheduleManager scheduleManager,
                                final ExecutorService service,
                                final ReplicaSetCache replicaSetCache)
    {
        myEccNodesSync = eccNodesSync;
        myJmxConnectionProvider = jmxConnectionProvider;
        myAgentNativeConnectionProvider = agentNativeConnectionProvider;
        myWorkerManager = workerManager;
        myScheduleManager = scheduleManager;
        myService = service;
        myReplicaSetCache = replicaSetCache;
    }

    /**
     * Handle a node switching state to UP.
     */
    public void onUp(final Node node)
    {
        LOG.debug("{} switched state to UP.", node);
        if (myAgentNativeConnectionProvider == null
                || !myAgentNativeConnectionProvider.getNodes().containsKey(node.getHostId()))
        {
            onAdd(node);
        }
        else
        {
            LOG.info("Node {} came back up, refreshing JMX connection in case endpoint changed", node.getHostId());
            NodeAddedAction callable = new NodeAddedAction(myEccNodesSync, myJmxConnectionProvider,
                    myAgentNativeConnectionProvider, node);
            myService.submit(callable);
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
     * Handle a node switching state to DOWN.
     */
    public void onDown(final Node node)
    {
        LOG.debug("{} switched state to DOWN.", node);
    }

    /**
     * Handle a new node being added to the cluster.
     */
    public void onAdd(final Node node)
    {
        if (myAgentNativeConnectionProvider == null || myAgentNativeConnectionProvider.confirmNodeValid(node))
        {
            LOG.info("Node added {}", node.getHostId());
            LongTokenRange.clearCache();
            clearReplicaSetCache();
            NodeAddedAction callable = new NodeAddedAction(myEccNodesSync, myJmxConnectionProvider,
                    myAgentNativeConnectionProvider, node);
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
     * Handle a node being removed from the cluster.
     */
    public void onRemove(final Node node)
    {
        if (myAgentNativeConnectionProvider == null || myAgentNativeConnectionProvider.confirmNodeValid(node))
        {
            LOG.info("Node removed {}", node.getHostId());
            LongTokenRange.clearCache();
            clearReplicaSetCache();
            NodeRemovedAction callable = new NodeRemovedAction(myEccNodesSync, myJmxConnectionProvider,
                    myAgentNativeConnectionProvider, node);
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

    private void clearReplicaSetCache()
    {
        if (myReplicaSetCache != null)
        {
            myReplicaSetCache.clear();
        }
    }
}
