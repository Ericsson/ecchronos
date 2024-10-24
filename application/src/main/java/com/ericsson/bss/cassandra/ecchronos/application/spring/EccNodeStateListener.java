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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;

import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EccNodeStateListener implements NodeStateListener
{
    private static final Integer NO_OF_THREADS = 1;
    private EccNodesSync myEccNodesSync;
    private DistributedJmxConnectionProvider myJmxConnectionProvider;
    private final ExecutorService service;
    private AgentNativeConnectionProvider myAgentNativeConnectionProvider = null;

    private static final Logger LOG = LoggerFactory.getLogger(EccNodeStateListener.class);

    public EccNodeStateListener()
    {
        service = Executors.newFixedThreadPool(NO_OF_THREADS);
    }

    /**
     * Invoked when a node is first added to the cluster.
     * @param node
     */
    @Override
    public void onAdd(final Node node)
    {
        LOG.info("Node added {}", node.getHostId());
        NodeAddedAction callable = new NodeAddedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
        service.submit(callable);
    }

    /**
     * Invoked when a node's state switches to NodeState.UP.
     * @param node
     */
    @Override
    public void onUp(final Node node)
    {
        LOG.info("Node up {}", node.getHostId());
    }

    /**
     * Invoked when a node's state switches to NodeState.DOWN or NodeState.FORCED_DOWN.
     * @param node
     */
    @Override
    public void onDown(final Node node)
    {
        LOG.info("Node Down {}", node.getHostId());
    }

    /**
     * Invoked when a node leaves the cluster.
     * @param node
     */
    @Override
    public void onRemove(final Node node)
    {
        LOG.info("Node removed {}", node.getHostId());
        NodeRemovedAction callable = new NodeRemovedAction(myEccNodesSync, myJmxConnectionProvider, myAgentNativeConnectionProvider, node);
        service.submit(callable);
    }

    @Override
    public void close() throws Exception
    {
    }

    /**
     * jmxConnectionProvider setter.
     * @param jmxConnectionProvider
     */
    public void setJmxConnectionProvider(final DistributedJmxConnectionProvider jmxConnectionProvider)
    {
        this.myJmxConnectionProvider = jmxConnectionProvider;
    }

    /**
     * agentNativeConnectionProvider setter.
     * @param agentNativeConnectionProvider
     */

    public void setAgentNativeConnectionProvider(final AgentNativeConnectionProvider agentNativeConnectionProvider)
    {
        this.myAgentNativeConnectionProvider = agentNativeConnectionProvider;
    }

    /**
     * eccNodesSync setter.
     * @param eccNodesSync
     */
    public void setEccNodesSync(final EccNodesSync eccNodesSync)
    {
        this.myEccNodesSync = eccNodesSync;
    }

}
