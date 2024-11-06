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
package com.ericsson.bss.cassandra.ecchronos.core.impl.refresh;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import com.datastax.oss.driver.api.core.metadata.Node;

public class NodeAddedAction implements Callable<Boolean>
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeAddedAction.class);

    private final EccNodesSync myEccNodesSync;
    private final DistributedJmxConnectionProvider myJmxConnectionProvider;
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;
    private final Node myNode;

    public NodeAddedAction(final EccNodesSync eccNodesSync, final DistributedJmxConnectionProvider jmxConnectionProvider, final DistributedNativeConnectionProvider distributedNativeConnectionProvider, final Node node)
    {
        myEccNodesSync = eccNodesSync;
        myJmxConnectionProvider = jmxConnectionProvider;
        myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
        myNode = node;
    }

    /**
     * Adds the node.
     * @return
     */
    @Override
    public Boolean call()
    {
        Boolean result = true;

        if (!myDistributedNativeConnectionProvider.confirmNodeValid(myNode))
        {
            return result;
        }

        LOG.info("Node Up {}", myNode.getHostId());
        myEccNodesSync.verifyAcquireNode(myNode);
        try
        {
            myJmxConnectionProvider.add(myNode);
        }
        catch (IOException e)
        {
            LOG.warn("Node {} JMX connection failed", myNode.getHostId());
            result = false;
        }
        myDistributedNativeConnectionProvider.addNode(myNode);

        return result;
    }
}
