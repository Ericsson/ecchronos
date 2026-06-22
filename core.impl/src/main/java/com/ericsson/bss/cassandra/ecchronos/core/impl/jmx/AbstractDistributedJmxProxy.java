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
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;

import org.jolokia.client.jmxadapter.UncheckedJmxAdapterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for distributed JMX proxy implementations.
 * Contains all transport-agnostic method implementations.
 */
abstract class AbstractDistributedJmxProxy implements DistributedJmxProxy
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDistributedJmxProxy.class);
    static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    static final String RS_OBJ_NAME = "org.apache.cassandra.db:type=RepairService";
    static final String LIVE_NODES_ATTRIBUTE = "LiveNodes";
    static final String UNREACHABLE_NODES_ATTRIBUTE = "UnreachableNodes";
    static final String FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD = "forceTerminateAllRepairSessions";
    static final String REPAIR_ASYNC_METHOD = "repairAsync";
    static final String REPAIR_STATS_METHOD = "getRepairStats";

    private final DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
    private final Map<UUID, Node> myNodesMap;
    private final EccNodesSync myEccNodesSync;
    private final ObjectName myStorageServiceObject;
    private final ObjectName myRepairServiceObject;
    private final ConcurrentHashMap<UUID, ReentrantLock> myNodeLocks = new ConcurrentHashMap<>();

    AbstractDistributedJmxProxy(
            final DistributedJmxConnectionProvider distributedJmxConnectionProvider,
            final Map<UUID, Node> nodesMap,
            final EccNodesSync eccNodesSync) throws MalformedObjectNameException
    {
        myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
        myNodesMap = nodesMap;
        myEccNodesSync = eccNodesSync;
        myStorageServiceObject = new ObjectName(SS_OBJ_NAME);
        myRepairServiceObject = new ObjectName(RS_OBJ_NAME);
    }

    protected DistributedJmxConnectionProvider getConnectionProvider()
    {
        return myDistributedJmxConnectionProvider;
    }

    protected Map<UUID, Node> getNodesMap()
    {
        return myNodesMap;
    }

    protected ObjectName getStorageServiceObject()
    {
        return myStorageServiceObject;
    }

    protected ObjectName getRepairServiceObject()
    {
        return myRepairServiceObject;
    }

    protected ReentrantLock getNodeLock(final UUID nodeID)
    {
        return myNodeLocks.computeIfAbsent(nodeID, id -> new ReentrantLock());
    }

    @Override
    public void close()
    {
        // NOOP - JolokiaNotificationController lifecycle is managed externally
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getLiveNodes(final UUID nodeID)
    {
        JMXConnector nodeConnection;
        UUID nodeIdConnection = nodeID;
        if (!myNodesMap.containsKey(nodeID))
        {
            LOG.debug("Node {} is not managed by local instance, using random connection to get live nodes", nodeID);
            Optional<UUID> firstNode = myDistributedJmxConnectionProvider.getJmxConnections().keySet().stream().findFirst();
            if (firstNode.isEmpty())
            {
                LOG.warn("No JMX connections available, cannot get live nodes for {}", nodeID);
                throw new IllegalStateException("No JMX connections available to query live nodes for node " + nodeID);
            }
            nodeIdConnection = firstNode.get();
            nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeIdConnection);
        }
        else
        {
            nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        }
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeIdConnection);
                lock.lock();
                try
                {
                    return (List<String>) nodeConnection
                            .getMBeanServerConnection()
                            .getAttribute(myStorageServiceObject,
                                    LIVE_NODES_ATTRIBUTE);
                }
                finally
                {
                    lock.unlock();
                }
            }
            catch (InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | AttributeNotFoundException
                   | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeIdConnection, e);
                LOG.error("Unable to get live nodes for node {}", nodeID, e);
            }
        }
        else
        {
            markNodeAsUnavailable(nodeIdConnection);
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> getUnreachableNodes(final UUID nodeID)
    {
        JMXConnector nodeConnection;
        UUID nodeIdConnection = nodeID;
        if (!myNodesMap.containsKey(nodeID))
        {
            LOG.debug("Node {} is not managed by local instance, using random connection to get unreachable nodes", nodeID);
            Optional<UUID> firstNode = myDistributedJmxConnectionProvider.getJmxConnections().keySet().stream().findFirst();
            if (firstNode.isEmpty())
            {
                LOG.warn("No JMX connections available, cannot get unreachable nodes for {}", nodeID);
                throw new IllegalStateException("No JMX connections available to query unreachable nodes for node " + nodeID);
            }
            nodeIdConnection = firstNode.get();
            nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeIdConnection);
        }
        else
        {
            nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        }

        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeIdConnection);
                lock.lock();
                try
                {
                    return (List<String>) nodeConnection
                            .getMBeanServerConnection()
                            .getAttribute(myStorageServiceObject,
                                    UNREACHABLE_NODES_ATTRIBUTE);
                }
                finally
                {
                    lock.unlock();
                }
            }
            catch (InstanceNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | AttributeNotFoundException
                   | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeIdConnection, e);
                LOG.error("Unable to get unreachable nodes for node {}", nodeID, e);
            }
        }
        else
        {
            markNodeAsUnavailable(nodeIdConnection);
        }
        return Collections.emptyList();
    }

    @Override
    public int repairAsync(
            final UUID nodeID,
            final String keyspace,
            final Map<String, String> options)
    {
        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                Object result;
                try
                {
                    result = nodeConnection
                            .getMBeanServerConnection().invoke(myStorageServiceObject,
                                    REPAIR_ASYNC_METHOD,
                                    new Object[]
                                            {
                                                    keyspace, options
                                            },
                                    new String[]
                                            {
                                                    String.class.getName(), Map.class.getName()
                                            });
                }
                finally
                {
                    lock.unlock();
                }
                LOG.debug("JMXRepair called for {} with options {}", keyspace, options);
                // Handle both Integer and Long return types from Jolokia
                if (result instanceof Number)
                {
                    return ((Number) result).intValue();
                }
                else
                {
                    LOG.warn("Unexpected return type from repairAsync: {}", result.getClass().getSimpleName());
                    return 0;
                }
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.error("Unable to repair node {}", nodeID, e);
            }
        }
        else
        {
            markNodeAsUnavailable(nodeID);
        }
        return 0;
    }

    /**
     * Force terminate all repair sessions in all nodes.
     */
    @Override
    public void forceTerminateAllRepairSessions()
    {
        for (
                Map.Entry<UUID, JMXConnector> entry
                :
                myDistributedJmxConnectionProvider.getJmxConnections().entrySet())
        {
            forceTerminateAllRepairSessionsInSpecificNode(entry.getKey());
        }
    }

    @Override
    public void forceTerminateAllRepairSessionsInSpecificNode(final UUID nodeID)
    {
        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                try
                {
                    nodeConnection
                            .getMBeanServerConnection().invoke(myStorageServiceObject,
                                    FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD,
                                    null, null);
                }
                finally
                {
                    lock.unlock();
                }
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.error("Unable to terminate repair sessions for node {}", nodeID, e);
            }
        }
        else
        {
            LOG.error("Unable to terminate repair sessions for node {} because the connection is unavailable", nodeID);
            markNodeAsUnavailable(nodeID);
        }
    }

    /**
     * Get the live disk space used.
     *
     * @param tableReference
     *         The table to get the live disk space for.
     * @return long
     */
    @Override
    public long liveDiskSpaceUsed(
            final UUID nodeID,
            final TableReference tableReference)
    {
        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);

        if (isConnectionAvailable)
        {
            try
            {
                ObjectName objectName
                        = new ObjectName(String
                        .format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=LiveDiskSpaceUsed",
                                tableReference.getKeyspace(), tableReference.getTable()));

                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                Object result;
                try
                {
                    result = nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Count");
                }
                finally
                {
                    lock.unlock();
                }
                if (result instanceof Number)
                {
                    return ((Number) result).longValue();
                }
                else
                {
                    LOG.warn("Unexpected type for LiveDiskSpaceUsed: {} for table {} in node {}",
                            result.getClass().getSimpleName(), tableReference, nodeID);
                    return 0L;
                }
            }
            catch (AttributeNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | MalformedObjectNameException
                   | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.error("Unable to retrieve disk space usage for table {} in node {}", tableReference,
                        nodeID,
                        e);
            }
            catch (RuntimeMBeanException
                   | InstanceNotFoundException e)
            {
                rethrowIfOutOfMemory(e);
                // This exception can occur when a table is about to be changed, but has not finished doing so,
                // before we try to fetch data from it. Just return the default value for the time being.
                LOG.warn("Unable to retrieve disk space usage for {} (bean not yet ready)", tableReference);
            }
        }
        else
        {
            LOG.error("Unable to retrieve disk space usage for table {} in node {} because the connection is unavailable",
                    tableReference,
                    nodeID);
            markNodeAsUnavailable(nodeID);
        }
        return 0;
    }

    @Override
    public double getPercentRepaired(
            final UUID nodeID,
            final TableReference tableReference)
    {
        // Check if this node is in our managed nodes map
        if (!myNodesMap.containsKey(nodeID))
        {
            LOG.info("Node {} is not in managed nodes map, skipping getPercentRepaired", nodeID);
            return 0.0;
        }

        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ObjectName objectName
                        = new ObjectName(String
                        .format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=PercentRepaired",
                                tableReference.getKeyspace(), tableReference.getTable()));

                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                Object result;
                try
                {
                    result = nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Value");
                }
                finally
                {
                    lock.unlock();
                }
                // Handle different numeric types that Jolokia might return
                if (result instanceof Number)
                {
                    return ((Number) result).doubleValue();
                }
                else if (result instanceof Double)
                {
                    return (Double) result;
                }
                else
                {
                    LOG.warn("Unexpected type for PercentRepaired: {} for table {} in node {}",
                            result.getClass().getSimpleName(), tableReference, nodeID);
                    return 0.0;
                }
            }
            catch (AttributeNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | MalformedObjectNameException
                   | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.error("Unable to retrieve percent repaired for {} in node {}",
                        tableReference, nodeID, e);
            }
            catch (ClassCastException e)
            {
                rethrowIfOutOfMemory(e);
                LOG.error("ClassCastException when getting percent repaired for table {} in node {} (Jolokia enabled: {})",
                        tableReference, nodeID, isJolokiaEnabled(), e);
            }
            catch (RuntimeMBeanException
                   | InstanceNotFoundException e)
            {
                rethrowIfOutOfMemory(e);
                // This exception can occur when a table is about to be changed, but has not finished doing so
                // before we try to fetch data from it. Just return the default value for the time being.
                LOG.warn("Unable to retrieve PercentRepaired for {} (bean not yet ready)", tableReference);
            }
        }
        else
        {
            markNodeAsUnavailable(nodeID);
        }
        return 0.0;
    }

    @Override
    public String getNodeStatus(final UUID nodeID)
    {
        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                try
                {
                    return (String) nodeConnection
                            .getMBeanServerConnection().getAttribute(myStorageServiceObject, "OperationMode");
                }
                finally
                {
                    lock.unlock();
                }
            }
            catch (InstanceNotFoundException
                   | AttributeNotFoundException
                   | MBeanException
                   | ReflectionException
                   | IOException
                   | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.error("Unable to retrieve node status for {}", nodeID, e);
            }
        }
        else
        {
            markNodeAsUnavailable(nodeID);
        }
        return "Unknown";
    }

    @Override
    public boolean isRepairActive(final UUID nodeID, final int command)
    {
        JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
        if (isConnectionAvailable)
        {
            try
            {
                ReentrantLock lock = getNodeLock(nodeID);
                lock.lock();
                @SuppressWarnings("unchecked")
                List<String> status;
                try
                {
                    status = (List<String>) nodeConnection
                            .getMBeanServerConnection().invoke(
                                    myStorageServiceObject,
                                    "getParentRepairStatus",
                                    new Object[]{command},
                                    new String[]{int.class.getName()});
                }
                finally
                {
                    lock.unlock();
                }

                LOG.debug("Parent repair session {} status on node {}: {}", command, nodeID, status);

                return status != null && "IN_PROGRESS".equals(status.get(0));
            }
            catch (Exception e)
            {
                rethrowIfOutOfMemory(e);
                invalidateIfConnectionStale(nodeID, e);
                LOG.warn("Unable to check active repair status for command {} on node {}, assuming still active",
                        command, nodeID, e);
                return true;
            }
        }
        else
        {
            markNodeAsUnavailable(nodeID);
        }
        return true;
    }

    protected Object invokeRepairStats(final UUID nodeID, final JMXConnector nodeConnection, final TableReference tableReference)
            throws InstanceNotFoundException, MBeanException, ReflectionException, IOException
    {
        List<String> args = new ArrayList<>();
        args.add(tableReference.getKeyspace());
        args.add(tableReference.getTable());
        ReentrantLock lock = getNodeLock(nodeID);
        lock.lock();
        try
        {
            return nodeConnection.getMBeanServerConnection().invoke(
                    myRepairServiceObject, REPAIR_STATS_METHOD,
                    new Object[]{args, null},
                    new String[]{List.class.getName(), String.class.getName()});
        }
        finally
        {
            lock.unlock();
        }
    }

    protected void markNodeAsUnavailable(final UUID nodeID)
    {
        Node node = myNodesMap.get(nodeID);
        if (node != null)
        {
            LOG.error("Unable to get connection with node {}, marking as UNREACHABLE", nodeID);
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, node.getDatacenter(), nodeID);
        }
        else
        {
            LOG.info("Node {} is not managed by local instance, cannot mark as unavailable", nodeID);
        }
    }

    /**
     * Invalidate the JMX connection for a node, forcing reconnection on next use.
     * Called when HTTP timeouts indicate the cached connection points to a stale address.
     */
    protected void invalidateConnection(final UUID nodeID)
    {
        try
        {
            getConnectionProvider().close(nodeID);
            LOG.info("Invalidated JMX connection for node {} to force reconnection", nodeID);
        }
        catch (IOException e)
        {
            LOG.debug("Error closing stale connection for node {}", nodeID, e);
        }
    }

    /**
     * Check if the exception indicates a stale connection (HTTP connect timeout)
     * and invalidate the connection if so.
     */
    protected void invalidateIfConnectionStale(final UUID nodeID, final Throwable t)
    {
        Throwable cause = t;
        while (cause != null)
        {
            if (cause instanceof HttpConnectTimeoutException
                    || cause instanceof ConnectException)
            {
                invalidateConnection(nodeID);
                return;
            }
            cause = cause.getCause();
        }
    }

    protected static void rethrowIfOutOfMemory(final Throwable t)
    {
        Throwable cause = t;
        while (cause != null)
        {
            if (cause instanceof OutOfMemoryError)
            {
                LOG.error("Rethrowing OutOfMemoryError", t);
                throw (OutOfMemoryError) cause;
            }
            cause = cause.getCause();
        }
    }

    /**
     * Returns whether Jolokia is enabled for this proxy. Used in log messages.
     */
    protected abstract boolean isJolokiaEnabled();

    @Override
    public void removeStorageServiceListener(
            final UUID nodeID,
            final NotificationListener listener)
    {
        JMXConnector nodeConnection = getConnectionProvider().getJmxConnector(nodeID);
        boolean isConnectionAvailable = validateJmxConnection(nodeConnection);

        if (isConnectionAvailable)
        {
            try
            {
                nodeConnection.removeConnectionNotificationListener(listener);
            }
            catch (ListenerNotFoundException e)
            {
                rethrowIfOutOfMemory(e);
                LOG.warn("Unable to remove connection notification listener for node {}", nodeID, e);
            }
            removeServiceListenerInternal(nodeID, listener, nodeConnection);
        }
        else
        {
            LOG.error("Unable to remove StorageService listener for node {} because the connection is unavailable", nodeID);
            markNodeAsUnavailable(nodeID);
        }
    }

    /**
     * Transport-specific removal of the service listener.
     */
    protected abstract void removeServiceListenerInternal(UUID nodeID, NotificationListener listener, JMXConnector nodeConnection);

    @SuppressWarnings("unchecked")
    @Override
    public long getMaxRepairedAt(
            final UUID nodeID,
            final TableReference tableReference)
    {
        if (!getNodesMap().containsKey(nodeID))
        {
            LOG.info("Node {} is not in managed nodes map, skipping getMaxRepairedAt", nodeID);
            return 0;
        }

        JMXConnector nodeConnection = getConnectionProvider().getJmxConnector(nodeID);
        if (!validateJmxConnection(nodeConnection))
        {
            LOG.error("Unable to get maxRepaired for table {} in node {} because the connection is unavailable",
                    tableReference, nodeID);
            markNodeAsUnavailable(nodeID);
            return 0;
        }

        try
        {
            Object result = invokeRepairStats(nodeID, nodeConnection, tableReference);
            return RepairStatsParser.extractMaxRepairedValue(result, isJolokiaEnabled());
        }
        catch (MBeanException | ReflectionException | IOException | UncheckedJmxAdapterException e)
        {
            rethrowIfOutOfMemory(e);
            invalidateIfConnectionStale(nodeID, e);
            LOG.error("Unable to get maxRepaired for table {} in node {}", tableReference, nodeID, e);
        }
        catch (ClassCastException e)
        {
            rethrowIfOutOfMemory(e);
            LOG.error("ClassCastException when getting maxRepaired for table {} in node {} (Jolokia enabled: {})",
                    tableReference, nodeID, isJolokiaEnabled(), e);
        }
        catch (RuntimeMBeanException
               | InstanceNotFoundException e)
        {
            rethrowIfOutOfMemory(e);
            LOG.warn("Unable to get maxRepaired for {} (bean not yet ready)", tableReference);
        }
        return 0;
    }
}
