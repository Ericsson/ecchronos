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

import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import javax.management.ObjectName;
import java.io.IOException;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;

import org.jolokia.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory creating JMX proxies to Cassandra.
 */
public final class DistributedJmxProxyFactoryImpl implements DistributedJmxProxyFactory
{
    private static final int DEFAULT_JOLOKIA_PORT = 8778;
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
    private final int jolokiaPort;

        private DistributedJmxProxyFactoryImpl(final Builder builder)
        {
            myDistributedJmxConnectionProvider = builder.myDistributedJmxConnectionProvider;
            nodesMap = builder.myNodesMap;
            eccNodesSync = builder.myEccNodesSync;
            isJolokiaEnabled = builder.isJolokiaEnabled;
            jolokiaPort = builder.myJolokiaPort;
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
                        jolokiaPort);
            }
            catch (MalformedObjectNameException e)
            {
                throw new IOException("Unable to get StorageService object", e);
            }
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

            private InternalDistributedJmxProxy(
                    final DistributedJmxConnectionProvider distributedJmxConnectionProvider,
                    final Map<UUID, Node> nodesMap,
                    final EccNodesSync eccNodesSync,
                    final boolean jolokiaEnabled,
                    final int jolokiaPortValue
            ) throws MalformedObjectNameException
            {
                myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
                myNodesMap = nodesMap;
                myEccNodesSync = eccNodesSync;
                myStorageServiceObject = new ObjectName(SS_OBJ_NAME);
                myRepairServiceObject = new ObjectName(RS_OBJ_NAME);
                isJolokiaEnabled = jolokiaEnabled;
                myJolokiaNotificationController = new JolokiaNotificationController(myNodesMap, jolokiaPortValue);
            }

        @Override
        public void close()
        {
            // Cancel all notification monitors
            myJolokiaNotificationController.close();
        }

        @Override
        public void addStorageServiceListener(final UUID nodeID, final NotificationListener listener)
        {
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
                        nodeConnection.getMBeanServerConnection().addNotificationListener(myStorageServiceObject, listener, null, null);
                    }
                }
                catch (InstanceNotFoundException | IOException | InterruptedException e)
                {
                    LOG.error("Unable to add StorageService listener in node {} with because of {}", nodeID, e.getMessage());
                }
            }
            else
            {
                markNodeAsUnavailable(nodeID);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<String> getLiveNodes(final UUID nodeID)
        {
            JMXConnector nodeConnection;
            UUID nodeIdConnection = nodeID;
            if (!myNodesMap.containsKey(nodeID))
            {
                LOG.info("Node {} is not managed by local instance, using random connection to get live nodes", nodeID);
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
                    return (List<String>) nodeConnection
                            .getMBeanServerConnection()
                            .getAttribute(myStorageServiceObject,
                                    LIVE_NODES_ATTRIBUTE);
                }
                catch (InstanceNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException
                       | AttributeNotFoundException e)
                {
                    LOG.error("Unable to get live nodes for node {} because of {}", nodeID, e.getMessage());
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
                LOG.info("Node {} is not managed by local instance, using random connection to get unreachable nodes", nodeID);
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
                    return (List<String>) nodeConnection
                            .getMBeanServerConnection()
                            .getAttribute(myStorageServiceObject,
                                    UNREACHABLE_NODES_ATTRIBUTE);
                }
                catch (InstanceNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException
                       | AttributeNotFoundException e)
                {
                    LOG.error("Unable to get unreachable nodes for node {} because of {}", nodeID, e.getMessage());
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
                    int result =  (int) nodeConnection
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
                    LOG.debug("JMXRepair called for {} with options {}", keyspace, options);
                    return result;
                }
                catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
                {
                    LOG.error("Unable to repair node {} because of {}", nodeID, e.getMessage());
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
                    nodeConnection
                            .getMBeanServerConnection().invoke(myStorageServiceObject,
                                    FORCE_TERMINATE_ALL_REPAIR_SESSIONS_METHOD,
                                    null, null);
                }
                catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
                {
                    LOG.error("Unable to terminate repair sessions for node {} because of {}", nodeID, e.getMessage());
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
                        nodeConnection.getMBeanServerConnection().removeNotificationListener(myStorageServiceObject, listener);
                    }

                }
                catch (InstanceNotFoundException | ListenerNotFoundException | IOException e)
                {
                    LOG.error("Unable to remove StorageService listener for node {} because of {}", nodeID, e.getMessage());
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

                    Object result = nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Count");
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
                       | InstanceNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException
                       | MalformedObjectNameException e)
                {
                    LOG.error("Unable to retrieve disk space usage for table {} in node {} because of {}", tableReference,
                            nodeID,
                            e.getMessage());
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
                Object result = invokeRepairStats(nodeConnection, tableReference);
                return extractMaxRepairedValue(result);
            }
            catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
            {
                LOG.error("Unable to get maxRepaired for table {} in node {} because of {}", tableReference, nodeID, e.getMessage());
            }
            catch (ClassCastException e)
            {
                LOG.error("ClassCastException when getting maxRepaired for table {} in node {} (Jolokia enabled: {}): {}",
                        tableReference, nodeID, isJolokiaEnabled, e.getMessage());
            }
            return 0;
        }

        private Object invokeRepairStats(final JMXConnector nodeConnection, final TableReference tableReference)
                throws InstanceNotFoundException, MBeanException, ReflectionException, IOException
        {
            List<String> args = new ArrayList<>();
            args.add(tableReference.getKeyspace());
            args.add(tableReference.getTable());
            return nodeConnection.getMBeanServerConnection().invoke(
                    myRepairServiceObject, REPAIR_STATS_METHOD,
                    new Object[]{args, null},
                    new String[]{List.class.getName(), String.class.getName()});
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

                    Object result = nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Value");
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
                       | InstanceNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException
                       | MalformedObjectNameException e)
                {
                    LOG.error("Unable to retrieve percent repaired for {} in node {}, because of {}",
                            tableReference, nodeID, e.getMessage());
                }
                catch (ClassCastException e)
                {
                    LOG.error("ClassCastException when getting percent repaired for table {} in node {} (Jolokia enabled: {}): {}",
                            tableReference, nodeID, isJolokiaEnabled, e.getMessage());
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
                    return (String) nodeConnection
                            .getMBeanServerConnection().getAttribute(myStorageServiceObject, "OperationMode");
                }
                catch (InstanceNotFoundException
                       | AttributeNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException e)
                {
                    LOG.error("Unable to retrieve node status for {} because of {}", nodeID, e.getMessage());
                }
            }
            else
            {
                markNodeAsUnavailable(nodeID);
            }
            return "Unknown";
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
                    LOG.debug("MBeanServerConnection not available: {}", e.getMessage());
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

    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
        private Map<UUID, Node> myNodesMap;
        private EccNodesSync myEccNodesSync;
        private boolean isJolokiaEnabled = false;
        private int myJolokiaPort = DEFAULT_JOLOKIA_PORT;

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
         * Build with Jolokia Agent.
         *
         * @param jolokiaPort Define what is the Jolokia port.
         * @return Builder
         */
        public Builder withJolokiaPort(final int jolokiaPort)
        {
            myJolokiaPort = jolokiaPort;
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
            return new DistributedJmxProxyFactoryImpl(this);
        }
    }
}
