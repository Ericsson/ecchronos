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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;

import org.jolokia.client.jmxadapter.UncheckedJmxAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RMI-based JMX proxy implementation.
 */
final class RMIJmxProxy extends AbstractDistributedJmxProxy
{
    private static final Logger LOG = LoggerFactory.getLogger(RMIJmxProxy.class);

    RMIJmxProxy(
            final DistributedJmxConnectionProvider distributedJmxConnectionProvider,
            final Map<UUID, Node> nodesMap,
            final EccNodesSync eccNodesSync) throws MalformedObjectNameException
    {
        super(distributedJmxConnectionProvider, nodesMap, eccNodesSync);
    }

    @Override
    public boolean addStorageServiceListener(final UUID nodeID, final NotificationListener listener)
    {
        boolean ret = true;
        JMXConnector nodeConnection = getConnectionProvider().getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                nodeConnection.addConnectionNotificationListener(listener, null, null);
                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                try
                {
                    nodeConnection.getMBeanServerConnection().addNotificationListener(getStorageServiceObject(), listener, null, null);
                }
                finally
                {
                    lock.unlock();
                }
            }
            catch (InstanceNotFoundException | IOException e)
            {
                rethrowIfOutOfMemory(e);
                LOG.error("Unable to add StorageService listener in node {}", nodeID, e);
                try
                {
                    nodeConnection.removeConnectionNotificationListener(listener);
                }
                catch (Exception removeEx)
                {
                    LOG.warn("Failed to remove connection notification listener during cleanup for node {}", nodeID, removeEx);
                }
                ret = false;
            }
        }
        else
        {
            markNodeAsUnavailable(nodeID);
            ret = false;
        }
        return ret;
    }

    @Override
    protected void removeServiceListenerInternal(final UUID nodeID, final NotificationListener listener,
            final JMXConnector nodeConnection)
    {
        try
        {
            ReentrantLock lock = getNodeLock(nodeID);
            lock.lock();
            try
            {
                nodeConnection.getMBeanServerConnection().removeNotificationListener(getStorageServiceObject(), listener);
            }
            finally
            {
                lock.unlock();
            }
        }
        catch (InstanceNotFoundException | ListenerNotFoundException | IOException | UncheckedJmxAdapterException e)
        {
            rethrowIfOutOfMemory(e);
            LOG.error("Unable to remove StorageService listener for node {}", nodeID, e);
        }
    }

    @Override
    protected boolean isJolokiaEnabled()
    {
        return false;
    }

    @Override
    public boolean validateJmxConnection(final JMXConnector jmxConnector)
    {
        return getConnectionProvider().isConnected(jmxConnector);
    }
}
