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
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.ClientRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationListenerResponse;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;

import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.management.Notification;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jolokia.client.J4pClient;
import org.jolokia.client.request.J4pExecRequest;

import org.jolokia.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory creating JMX proxies to Cassandra.
 */
@SuppressWarnings({"PMD.ClassWithOnlyPrivateConstructorsShouldBeFinal", "checkstyle:finalclass", "PMD.GodClass"})
public class DistributedJmxProxyFactoryImpl implements DistributedJmxProxyFactory
{
    private static final long DEFAULT_RUN_DELAY_IN_MS = 100;
    private static final String CLIENT_ID_PROPERTY = "clientID";
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

    private DistributedJmxProxyFactoryImpl(final Builder builder)
    {
        myDistributedJmxConnectionProvider = builder.myDistributedJmxConnectionProvider;
        nodesMap = builder.myNodesMap;
        eccNodesSync = builder.myEccNodesSync;
        isJolokiaEnabled = builder.isJolokiaEnabled;
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
                    isJolokiaEnabled);
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
        private final Map<NotificationListener, String> myJolokiaRelationshipListeners = new HashMap<>();

        private final ScheduledExecutorService myNotificationExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("NotificationRefresher-%d").build());

        private final Map<UUID, Map<String, String>> myClientIdMap = new ConcurrentHashMap<>();
        private final Map<UUID, Map<String, NotificationListener>> myNodeListenersMap = new ConcurrentHashMap<>();
        private final Map<UUID, Map<String, ScheduledFuture<?>>> myNotificationMonitors = new ConcurrentHashMap<>();

        private final boolean isJolokiaEnabled;
        private final EccNodesSync myEccNodesSync;
        private final ObjectName myStorageServiceObject;
        private final ObjectName myRepairServiceObject;

        private final ObjectMapper objectMapper = new ObjectMapper();
        private final HttpClient client = HttpClient.newHttpClient();

        private InternalDistributedJmxProxy(
                final DistributedJmxConnectionProvider distributedJmxConnectionProvider,
                final Map<UUID, Node> nodesMap,
                final EccNodesSync eccNodesSync,
                final boolean jolokiaEnabled
        ) throws MalformedObjectNameException
        {
            myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
            myNodesMap = nodesMap;
            myEccNodesSync = eccNodesSync;
            myStorageServiceObject = new ObjectName(SS_OBJ_NAME);
            myRepairServiceObject = new ObjectName(RS_OBJ_NAME);
            isJolokiaEnabled = jolokiaEnabled;
        }

        private void startNotificationMonitor(final UUID nodeID, final String notificationID)
        {
            ScheduledFuture<?> future = myNotificationExecutor.scheduleWithFixedDelay(
                    new NotificationRunTask(nodeID, notificationID), 0, DEFAULT_RUN_DELAY_IN_MS, TimeUnit.MILLISECONDS);

            synchronized (myNotificationMonitors)
            {
                myNotificationMonitors
                        .computeIfAbsent(nodeID, k -> new ConcurrentHashMap<>())
                        .put(notificationID, future);
            }
        }

        private final class NotificationRunTask implements Runnable
        {
            private final UUID myNodeID;
            private final String myNotificationID;
            private final Set<Integer> notificationController = new HashSet<>();

            private NotificationRunTask(final UUID nodeID, final String notificationID)
            {
                myNodeID = nodeID;
                myNotificationID = notificationID;
            }

            @Override
            public void run()
            {
                try
                {
                    JSONObject response = checkForNotifications(myNodeID, myNotificationID);
                    NotificationListenerResponse notificationListenerResponse = objectMapper.readValue(response.toJSONString(),
                            NotificationListenerResponse.class);

                    if (notificationListenerResponse.getValue() != null)
                    {
                        List<NotificationListenerResponse.Notification> notifications =
                                notificationListenerResponse.getValue().getNotifications();

                        for (NotificationListenerResponse.Notification notificationObj : notifications)
                        {
                            Notification notification = new Notification(
                                    notificationObj.getType(),
                                    notificationObj.getSource(),
                                    notificationObj.getTimeStamp(),
                                    notificationObj.getMessage()
                            );
                            notification.setUserData(notificationObj.getUserData());
                            if (!notificationController.contains(notification.hashCode()))
                            {
                                notificationController.add(notification.hashCode());
                                synchronized (myNodeListenersMap)
                                {
                                    NotificationListener listener = myNodeListenersMap.get(myNodeID).get(myNotificationID);
                                    if (listener != null)
                                    {
                                        listener.handleNotification(notification, null);
                                    }
                                    else
                                    {
                                        LOG.warn("Listener not found for node {} and notificationID {}", myNodeID, myNotificationID);
                                    }
                                }
                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    LOG.error("Error monitoring notifications for node {} and notificationID {}: {}", myNodeID, myNotificationID, e.getMessage());
                }
            }
        }

        private void registerClientId(final UUID nodeID)
        {
            try
            {
                String url = mountJolokiaBaseURL(nodeID) + "/notification/register";

                HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                ClientRegisterResponse clientRegisterResponse = objectMapper.readValue(response.body(),
                        ClientRegisterResponse.class);

                Map<String, String> properties = new HashMap<>();
                properties.put(CLIENT_ID_PROPERTY, clientRegisterResponse.getValue().getId());
                properties.put("store", clientRegisterResponse.getValue().getBackend().getPull().getStore());

                myClientIdMap.put(nodeID, properties);
            }
            catch (IOException | InterruptedException e)
            {
                LOG.error("Unable to register Jolokia Client in node with ID {} because of {}", nodeID, e.getMessage());
            }
        }

        private String registerJolokiaNotification(final UUID nodeID) throws IOException, InterruptedException
        {
            String url = mountJolokiaBaseURL(nodeID) + "/notification";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString(jolokiaCreateNotificationOptions(nodeID))).build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            NotificationRegisterResponse notificationRegisterResponse = objectMapper.readValue(response.body(),
                    NotificationRegisterResponse.class);

            return notificationRegisterResponse.getValue();
        }

        private void removeJolokiaNotification(final UUID nodeID, final String notificationID)
        {
            String url = mountJolokiaBaseURL(nodeID) + "/notification";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString(
                        jolokiaRemoveNotificationOptions(nodeID, notificationID))).build();
            try
            {
                client.send(request, HttpResponse.BodyHandlers.ofString());
            }
            catch (IOException | InterruptedException e)
            {
                LOG.error("Error trying to remove NotificationListener with ID {} in node {}, because of {}",
                        notificationID, nodeID, e.getMessage());
            }
        }

        private String jolokiaCreateNotificationOptions(final UUID nodeID)
        {
            Map<String, Object> params = new HashMap<>();
            params.put("type", "notification");
            params.put("command", "add");
            params.put("client", myClientIdMap.get(nodeID).get(CLIENT_ID_PROPERTY));
            params.put("mode", "pull");
            params.put("mbean", SS_OBJ_NAME);
            List<String> filter = List.of(
                    "progress",
                    "jmx.remote.connection.lost.notifications",
                    "jmx.remote.connection.failed",
                    "jmx.remote.connection.closed"
            );
            params.put("filter", filter);

            try
            {
                return objectMapper.writeValueAsString(params);
            }
            catch (JsonProcessingException e)
            {
                LOG.error("Unable to serialize notification options for node {} because of {}", nodeID, e.getMessage());
            }
            return "";
        }

        private String jolokiaRemoveNotificationOptions(final UUID nodeID, final String notificationID)
        {
            Map<String, Object> params = new HashMap<>();
            params.put("type", "notification");
            params.put("command", "remove");
            params.put("client", myClientIdMap.get(nodeID).get(CLIENT_ID_PROPERTY));
            params.put("handle", notificationID);

            try
            {
                return objectMapper.writeValueAsString(params);
            }
            catch (JsonProcessingException e)
            {
                LOG.error("Unable to serialize Jolokia Notification Options for node {} because of {}", nodeID,
                        e.getMessage());
            }
            return "";
        }

        private J4pClient mountJolokiaClient(final UUID nodeID)
        {
            return new J4pClient(mountJolokiaBaseURL(nodeID));
        }

        private String mountJolokiaBaseURL(final UUID nodeID)
        {
            String host = myNodesMap.get(nodeID).getBroadcastRpcAddress().get().getHostString();
            return "http://" + host + ":8778/jolokia";
        }

        private JSONObject checkForNotifications(final UUID nodeID, final String notificationID)
        {
            String operation = "pull";
            try
            {
                synchronized (myClientIdMap)
                {
                    J4pExecRequest execRequest = new J4pExecRequest(
                            myClientIdMap.get(nodeID).get("store"),
                            operation, myClientIdMap.get(nodeID).get(CLIENT_ID_PROPERTY),
                            notificationID);

                    return mountJolokiaClient(nodeID).execute(execRequest).asJSONObject();
                }
            }
            catch (Exception e)
            {
                LOG.error("Error checking notifications for node {} and notificationID {}: {}", nodeID, notificationID, e.getMessage());
            }
            return new JSONObject();
        }

        @Override
        public void close()
        {
            // Should not close
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
                        registerClientId(nodeID);

                        String jolokiaNotificationID = registerJolokiaNotification(nodeID);

                        synchronized (myNodeListenersMap)
                        {
                            myNodeListenersMap.computeIfAbsent(nodeID, k -> new ConcurrentHashMap<>()).put(jolokiaNotificationID, listener);
                        }

                        myJolokiaRelationshipListeners.put(listener, jolokiaNotificationID);

                        startNotificationMonitor(nodeID, jolokiaNotificationID);
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
            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
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
                markNodeAsUnavailable(nodeID);
            }
            return Collections.emptyList();
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<String> getUnreachableNodes(final UUID nodeID)
        {
            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
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
                markNodeAsUnavailable(nodeID);
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
                    return (int) nodeConnection
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
                        String jolokiaNotificationID = myJolokiaRelationshipListeners.get(listener);
                        removeJolokiaNotification(nodeID, jolokiaNotificationID);
                        synchronized (myNotificationMonitors)
                        {
                            myNotificationMonitors.get(nodeID).get(jolokiaNotificationID).cancel(true);
                            myNotificationMonitors.get(nodeID).remove(jolokiaNotificationID);
                        }
                        myNodeListenersMap.get(nodeID).remove(jolokiaNotificationID);
                        myJolokiaRelationshipListeners.remove(listener);
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

                    return (Long) nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Count");
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
            JMXConnector nodeConnection = myDistributedJmxConnectionProvider.getJmxConnector(nodeID);
            boolean isConnectionAvailable = validateJmxConnection(nodeConnection);
            if (isConnectionAvailable)
            {
                try
                {
                    List<String> args = new ArrayList<>();
                    args.add(tableReference.getKeyspace());
                    args.add(tableReference.getTable());
                    List<CompositeData> compositeDatas = (List<CompositeData>) nodeConnection
                            .getMBeanServerConnection().invoke(
                                    myRepairServiceObject, REPAIR_STATS_METHOD,
                                    new Object[]
                                            {
                                                    args, null
                                            },
                                    new String[]
                                            {
                                                    List.class.getName(),
                                                    String.class.getName()
                                            });
                    for (CompositeData data : compositeDatas)
                    {
                        return (long) data.getAll(new String[] {"maxRepaired"})[0];
                    }
                }
                catch (InstanceNotFoundException | MBeanException | ReflectionException | IOException e)
                {
                    LOG.error("Unable to get maxRepaired for table {} in node {} because of {}", tableReference, nodeID, e.getMessage());
                }
            }
            else
            {
                LOG.error("Unable to get maxRepaired for table {} in node {} because the connection is unavailable",
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

                    return (double) nodeConnection
                            .getMBeanServerConnection().getAttribute(objectName, "Value");
                }
                catch (AttributeNotFoundException
                       | InstanceNotFoundException
                       | MBeanException
                       | ReflectionException
                       | IOException
                       | MalformedObjectNameException e)
                {
                    LOG.error("Unable to retrieve disk space usage for {} in node {}, because of {}",
                            tableReference, nodeID, e.getMessage());
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
            return myDistributedJmxConnectionProvider.isConnected(jmxConnector);
        }

        private void markNodeAsUnavailable(final UUID nodeID)
        {
            LOG.error("Unable to get connection with node {}, marking as UNREACHABLE", nodeID);
            myEccNodesSync.updateNodeStatus(NodeStatus.UNAVAILABLE, myNodesMap.get(nodeID).getDatacenter(), nodeID);
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
