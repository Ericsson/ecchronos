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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;

import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.ObjectName;
import java.io.IOException;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;

import org.jolokia.client.jmxadapter.UncheckedJmxAdapterException;
import org.jolokia.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory creating JMX proxies to Cassandra.
 */
public final class  DistributedJmxProxyFactoryImpl implements DistributedJmxProxyFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedJmxProxyFactoryImpl.class);
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    private static final String RS_OBJ_NAME = "org.apache.cassandra.db:type=RepairService";
    private static final String LIVE_NODES_ATTRIBUTE = "LiveNodes";
    private static final String UNREACHABLE_NODES_ATTRIBUTE = "UnreachableNodes";
    private static final String FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD = "forceTerminateAllRepairSessions";
    private static final String REPAIR_ASYNC_METHOD = "repairAsync";
    private static final String REPAIR_STATS_METHOD = "getRepairStats";

    private final DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
    private final Map<UUID, Node> nodesMap;
    private final EccNodesSync eccNodesSync;
    private final boolean isJolokiaEnabled;
    private final Integer myMaxWaitTimeInMinutes;
    private final JolokiaNotificationController myJolokiaNotificationController;

    private DistributedJmxProxyFactoryImpl(final Builder builder)
    {
        myDistributedJmxConnectionProvider = builder.myDistributedJmxConnectionProvider;
        nodesMap = builder.myNodesMap;
        eccNodesSync = builder.myEccNodesSync;
        isJolokiaEnabled = builder.isJolokiaEnabled;
        myMaxWaitTimeInMinutes = builder.myMaxWaitTimeInMinutes;
        myJolokiaNotificationController = builder.myJolokiaController;
    }

    @Override
    public DistributedJmxProxy connect() throws IOException
    {
        try
        {
            return new InternalDistributedJmxProxy(
                    myDistributedJmxConnectionProvider,
                    nodesMap,
                    eccNodesSync,
                    isJolokiaEnabled,
                    myJolokiaNotificationController);
        }
        catch (MalformedObjectNameException e)
        {
            throw new IOException("Unable to get StorageService object", e);
        }
    }
    @Override
    public Integer getMaxWaitTimeInMinutes()
    {
        return myMaxWaitTimeInMinutes;
    }

    private static final class InternalDistributedJmxProxy implements DistributedJmxProxy
    {
        private final DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
        private final Map<UUID, Node> myNodesMap;

        private final boolean isJolokiaEnabled;
        private final EccNodesSync myEccNodesSync;
        private final ObjectName myStorageServiceObject;
        private final ObjectName myRepairServiceObject;
        private final JolokiaNotificationController myJolokiaNotificationController;
        private final ConcurrentHashMap<UUID, ReentrantLock> myNodeLocks = new ConcurrentHashMap<>();

        private InternalDistributedJmxProxy(
                final DistributedJmxConnectionProvider distributedJmxConnectionProvider,
                final Map<UUID, Node> nodesMap,
                final EccNodesSync eccNodesSync,
                final boolean jolokiaEnabled,
                final JolokiaNotificationController jolokiaNotificationController) throws MalformedObjectNameException
        {
            myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
            myNodesMap = nodesMap;
            myEccNodesSync = eccNodesSync;
            myStorageServiceObject = new ObjectName(SS_OBJ_NAME);
            myRepairServiceObject = new ObjectName(RS_OBJ_NAME);
            isJolokiaEnabled = jolokiaEnabled;
            myJolokiaNotificationController = jolokiaNotificationController;
        }

        private ReentrantLock getNodeLock(final UUID nodeID)
        {
            return myNodeLocks.computeIfAbsent(nodeID, id -> new ReentrantLock());
        }

        @Override
        public void close()
        {
            // NOOP - JolokiaNotificationController lifecycle is managed externally
        }

        @Override
        public boolean addStorageServiceListener(final UUID nodeID, final NotificationListener listener)
        {
            boolean ret = true;
            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
            boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
            if (isConnectionAvailable)
            {
                try
                {
                    nodeConnection.addConnectionNotificationListener(listener, null, null);
                    if (isJolokiaEnabled)
                    {
                        myJolokiaNotificationController.addStorageServiceListener(nodeID, listener);
                    }
                    else
                    {
                        ReentrantLock lock = getNodeLock(nodeID);
                        lock.lock();
                        try
                        {
                            nodeConnection.getMBeanServerConnection().addNotificationListener(myStorageServiceObject, listener, null, null);
                        }
                        finally
                        {
                            lock.unlock();
                        }
                    }
                }
                catch (InstanceNotFoundException | IOException | InterruptedException e)
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

        @SuppressWarnings("unchecked")
        @Override
        public List<String> getLiveNodes(final UUID nodeID)
        {
            JMXConnector nodeConnection;
            UUID nodeIdConnection = nodeID;
            if (!myNodesMap.containsKey(nodeID))
            {
                LOG.debug("Node {} is not managed by local instance, using random connection to get live nodes", nodeID);
                nodeIdConnection = myDistributedJmxConnectionProvider.getJmxConnections().keySet().stream().findFirst().get();
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
                nodeIdConnection = myDistributedJmxConnectionProvider.getJmxConnections().keySet().stream().findFirst().get();
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
         * Remove the storage service listener.
         *
         * @param listener
         *         The listener to remove.
         */
        @Override
        public void removeStorageServiceListener(
                final UUID nodeID,
                final NotificationListener listener)
        {
            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
            boolean isConnectionAvailable = validateJmxConnection(nodeConnection);

            if (isConnectionAvailable)
            {
                try
                {
                    nodeConnection.removeConnectionNotificationListener(listener);
                    if (isJolokiaEnabled)
                    {
                        myJolokiaNotificationController.removeStorageServiceListener(nodeID, listener);
                    }
                    else
                    {
                        ReentrantLock lock = getNodeLock(nodeID);
                        lock.lock();
                        try
                        {
                            nodeConnection.getMBeanServerConnection().removeNotificationListener(myStorageServiceObject, listener);
                        }
                        finally
                        {
                            lock.unlock();
                        }
                    }

                }
                catch (InstanceNotFoundException | ListenerNotFoundException | IOException | UncheckedJmxAdapterException e)
                {
                    rethrowIfOutOfMemory(e);
                    LOG.error("Unable to remove StorageService listener for node {}", nodeID, e);
                }
            }
            else
            {
                LOG.error("Unable to remove StorageService listener for node {} because the connection is unavailable", nodeID);
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

        @SuppressWarnings("unchecked")
        @Override
        public long getMaxRepairedAt(
                final UUID nodeID,
                final TableReference tableReference)
        {
            // Check if this node is in our managed nodes map
            if (!myNodesMap.containsKey(nodeID))
            {
                LOG.info("Node {} is not in managed nodes map, skipping getMaxRepairedAt", nodeID);
                return 0;
            }

            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
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
                return extractMaxRepairedValue(result);
            }
            catch (MBeanException | ReflectionException | IOException | UncheckedJmxAdapterException e)
            {
                rethrowIfOutOfMemory(e);
                LOG.error("Unable to get maxRepaired for table {} in node {}", tableReference, nodeID, e);
            }
            catch (ClassCastException e)
            {
                rethrowIfOutOfMemory(e);
                LOG.error("ClassCastException when getting maxRepaired for table {} in node {} (Jolokia enabled: {})",
                        tableReference, nodeID, isJolokiaEnabled, e);
            }
            catch (RuntimeMBeanException
                   | InstanceNotFoundException e)
            {
                rethrowIfOutOfMemory(e);
                // This exception can occur when a table is about to be changed, but has not finished doing so,
                // before we try to fetch data from it. Just return the default value for the time being.
                LOG.warn("Unable to get maxRepaired for {} (bean not yet ready)", tableReference);
            }
            return 0;
        }

        private Object invokeRepairStats(final UUID nodeID, final JMXConnector nodeConnection, final TableReference tableReference)
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

        @SuppressWarnings("unchecked")
        private long extractMaxRepairedValue(final Object result)
        {
            if (!(result instanceof List))
            {
                return 0;
            }

            List<?> resultList = (List<?>) result;
            if (isJolokiaEnabled)
            {
                return extractFromJolokiaResult(resultList);
            }
            return extractFromCompositeDataResult((List<CompositeData>) resultList);
        }

        private long extractFromJolokiaResult(final List<?> resultList)
        {
            for (Object item : resultList)
            {
                if (item instanceof JSONObject)
                {
                    long value = extractFromJsonObject((JSONObject) item);
                    if (value > 0)
                    {
                        return value;
                    }
                }
                else if (item instanceof CompositeData)
                {
                    return extractFromCompositeData((CompositeData) item);
                }
            }
            return 0;
        }

        private long extractFromJsonObject(final JSONObject jsonObj)
        {
            Object maxRepaired = jsonObj.get("maxRepaired");
            return (maxRepaired instanceof Number) ? ((Number) maxRepaired).longValue() : 0;
        }

        private long extractFromCompositeDataResult(final List<CompositeData> compositeDatas)
        {
            for (CompositeData data : compositeDatas)
            {
                return extractFromCompositeData(data);
            }
            return 0;
        }

        private long extractFromCompositeData(final CompositeData data)
        {
            return (long) data.getAll(new String[]{"maxRepaired"})[0];
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
                    LOG.error("Unable to retrieve percent repaired for {} in node {}",
                            tableReference, nodeID, e);
                }
                catch (ClassCastException e)
                {
                    rethrowIfOutOfMemory(e);
                    LOG.error("ClassCastException when getting percent repaired for table {} in node {} (Jolokia enabled: {})",
                            tableReference, nodeID, isJolokiaEnabled, e);
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

        @Override
        public boolean validateJmxConnection(final JMXConnector jmxConnector)
        {
            if (!myDistributedJmxConnectionProvider.isConnected(jmxConnector))
            {
                return false;
            }
            // Additional check for MBeanServerConnection when using Jolokia
            if (isJolokiaEnabled && jmxConnector != null)
            {
                try
                {
                    return jmxConnector.getMBeanServerConnection() != null;
                }
                catch (IOException e)
                {
                    LOG.debug("MBeanServerConnection not available", e);
                    return false;
                }
            }
            return true;
        }

        private void markNodeAsUnavailable(final UUID nodeID)
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

        private static void rethrowIfOutOfMemory(final Throwable t)
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

    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        public static final int DEFAULT_RUN_DELAY = 500;
        public static final int DEFAULT_MAX_WAIT_TIME_IN_MINUTES = 40;
        private DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
        private Map<UUID, Node> myNodesMap;
        private EccNodesSync myEccNodesSync;
        private boolean isJolokiaEnabled = false;
        private Integer myMaxWaitTimeInMinutes = DEFAULT_MAX_WAIT_TIME_IN_MINUTES;
        private IpTranslator myIpTranslator;
        private JolokiaNotificationController myJolokiaController;

        /**
         * Build with JMX connection provider.
         *
         * @param distributedJmxConnectionProvider The JMX connection provider
         * @return Builder
         */
        public Builder withJmxConnectionProvider(final DistributedJmxConnectionProvider distributedJmxConnectionProvider)
        {
            myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
            return this;
        }

        /**
         * Build with Nodes map.
         *
         * @param nodesMap The Nodes map
         * @return Builder
         */
        public Builder withNodesMap(final Map<UUID, Node> nodesMap)
        {
            myNodesMap = nodesMap;
            return this;
        }

        /**
         * Build with EccNodesSync.
         *
         * @param eccNodesSync The EccNodesSync
         * @return Builder
         */
        public Builder withEccNodesSync(final EccNodesSync eccNodesSync)
        {
            myEccNodesSync = eccNodesSync;
            return this;
        }

        /**
         * Build with Jolokia Agent.
         *
         * @param jolokiaEnabled Define if Jolokia Client must be used.
         * @return Builder
         */
        public Builder withJolokiaEnabled(final boolean jolokiaEnabled)
        {
            isJolokiaEnabled = jolokiaEnabled;
            return this;
        }

        /**
         * Build with maxWaitTimeInMinutes.
         *
         * @param maxWaitTimeInMinutes Integer
         * @return Builder
         */
        public Builder withMaxWaitTimeInMinutes(final Integer maxWaitTimeInMinutes)
        {
            myMaxWaitTimeInMinutes = maxWaitTimeInMinutes;
            return this;
        }

        /**
         * Build with IpTranslator.
         *
         * @param ipTranslator
         * @return Builder
         */
        public Builder withIpTranslator(final IpTranslator ipTranslator)
        {
            myIpTranslator = ipTranslator;
            return this;
        }

        /**
         * Build with JolokiaNotificationController.
         *
         * @param jolokiaNotificationController The Custom Notification Controller responsible for managing notifications in jolokia.
         * @return Builder
         */
        public Builder withJolokiaNotificationController(final JolokiaNotificationController jolokiaNotificationController)
        {
            myJolokiaController = jolokiaNotificationController;
            return this;
        }

        /**
         * Build.
         *
         * @return DistributedJmxProxyFactoryImpl
         */
        public DistributedJmxProxyFactoryImpl build()
        {
            if (myDistributedJmxConnectionProvider == null)
            {
                throw new IllegalArgumentException("JMX Connection provider cannot be null");
            }
            if (myIpTranslator == null)
            {
                throw new IllegalArgumentException("IpTranslator cannot be null");
            }
            return new DistributedJmxProxyFactoryImpl(this);
        }
    }
}
