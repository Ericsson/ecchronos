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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.ClientRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationListenerResponse;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JolokiaNotificationController
{
    private static final Logger LOG = LoggerFactory.getLogger(JolokiaNotificationController.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 1;
    private static final int HTTP_TIMEOUT_SECONDS = 5;
    private static final int HTTP_REQUEST_TIMEOUT_SECONDS = 10;
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_IN_MS = 500;
    private static final int MAX_CONSECUTIVE_FAILURES = 5;
    private static final String CLIENT_ID_PROPERTY = "clientID";
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    public static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP

    private final Map<NotificationListener, String> myJolokiaRelationshipListeners = new HashMap<>();

    private final ScheduledExecutorService myNotificationExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("NotificationRefresher-%d").build());

    private final Map<UUID, Map<String, String>> myClientIdMap = new ConcurrentHashMap<>();
    private final Map<UUID, Map<String, NotificationListener>> myNodeListenersMap = new ConcurrentHashMap<>();
    private final Map<UUID, Map<String, ScheduledFuture<?>>> myNotificationMonitors = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CertificateHandler myCertificateHandler;

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final int myJolokiaPort;
    private final boolean myJolokiaPEM;
    private final boolean myReverseDNSResolution;
    private final String myURLPrefix;
    private final long myRunDelay;
    private final IpTranslator myIpTranslator;

    public JolokiaNotificationController(final Builder builder)
    {
        myNativeConnectionProvider = builder.myNativeConnection;
        myJolokiaPort = builder.myJolokiaPort;
        myJolokiaPEM = builder.myJolokiaPEM;
        myURLPrefix = myJolokiaPEM ? "https" : "http";
        myReverseDNSResolution = builder.myReverseDNSResolution;
        myRunDelay = builder.myRunDelay;
        myIpTranslator = builder.myIpTranslator;
        myCertificateHandler = builder.myCertificateHandler;
    }

    private HttpClient buildHttpClient()
    {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(HTTP_TIMEOUT_SECONDS));
        if (myCertificateHandler != null)
        {
            SSLContext sslContext = myCertificateHandler.getSSLContext();
            if (sslContext != null)
            {
                builder.sslContext(sslContext);
            }
        }
        return builder.build();
    }

    public final void addStorageServiceListener(final UUID nodeID, final NotificationListener listener) throws IOException, InterruptedException
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

    public final void removeStorageServiceListener(
            final UUID nodeID,
            final NotificationListener listener) throws UnknownHostException
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

    private void startNotificationMonitor(final UUID nodeID, final String notificationID)
    {
        ScheduledFuture<?> future = myNotificationExecutor.scheduleWithFixedDelay(
                new NotificationRunTask(nodeID, notificationID), 0, myRunDelay, TimeUnit.MILLISECONDS);

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
        private int consecutiveFailures = 0;

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
                String response = checkForNotificationsWithRetry(myNodeID, myNotificationID);
                NotificationListenerResponse notificationListenerResponse = objectMapper.readValue(response,
                        NotificationListenerResponse.class);

                if (notificationListenerResponse.getValue() != null)
                {
                    List<NotificationListenerResponse.Notification> notifications =
                            notificationListenerResponse.getValue().getNotifications();

                    for (NotificationListenerResponse.Notification notificationObj : notifications)
                    {
                        createNotification(notificationObj);
                    }
                }
                consecutiveFailures = 0;
            }
            catch (Exception e)
            {
                consecutiveFailures++;
                if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES)
                {
                    LOG.error("Notification check failed {} consecutive times for node {} and notificationID {}. "
                            + "Signaling connection failure.", consecutiveFailures, myNodeID, myNotificationID);
                    signalConnectionFailure();
                }
                else
                {
                    LOG.warn("Transient notification check failure ({}/{}) for node {} and notificationID {}",
                            consecutiveFailures, MAX_CONSECUTIVE_FAILURES, myNodeID, myNotificationID, e);
                }
            }
        }

        private void signalConnectionFailure()
        {
            synchronized (myNodeListenersMap)
            {
                Map<String, NotificationListener> nodeListeners = myNodeListenersMap.get(myNodeID);
                if (nodeListeners != null)
                {
                    NotificationListener listener = nodeListeners.get(myNotificationID);
                    if (listener != null)
                    {
                        Notification failNotification = new Notification(
                                "jmx.remote.connection.failed",
                                "JolokiaNotificationController",
                                System.currentTimeMillis(),
                                "Jolokia communication failed after " + MAX_CONSECUTIVE_FAILURES + " consecutive attempts"
                        );
                        listener.handleNotification(failNotification, null);
                    }
                }
            }
        }

        private void createNotification(final NotificationListenerResponse.Notification notificationObj)
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
                    Map<String, NotificationListener> nodeListeners = myNodeListenersMap.get(myNodeID);
                    if (nodeListeners != null)
                    {
                        NotificationListener listener = nodeListeners.get(myNotificationID);
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

    private void registerClientId(final UUID nodeID)
    {
        try
        {
            String url = mountJolokiaBaseURL(nodeID) + "/notification/register";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                    .GET()
                    .build();

            HttpResponse<String> response = buildHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

            LOG.debug("Raw response from {}: Status={}, Body={}", url, response.statusCode(), response.body());

            ClientRegisterResponse clientRegisterResponse = objectMapper.readValue(
                    decodeHtmlEntities(response.body()), ClientRegisterResponse.class);

            Map<String, String> properties = new HashMap<>();
            properties.put(CLIENT_ID_PROPERTY, clientRegisterResponse.getValue().getId());
            properties.put("store", clientRegisterResponse.getValue().getBackend().getPull().getStore());

            myClientIdMap.put(nodeID, properties);
        }
        catch (IOException | InterruptedException e)
        {
            LOG.error("Unable to register Jolokia Client in node with ID {}", nodeID, e);
        }
    }

    private String registerJolokiaNotification(final UUID nodeID) throws IOException, InterruptedException
    {
        String url = mountJolokiaBaseURL(nodeID) + "/notification";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                .POST(HttpRequest.BodyPublishers.ofString(jolokiaCreateNotificationOptions(nodeID)))
                .build();
        HttpResponse<String> response = buildHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        NotificationRegisterResponse notificationRegisterResponse = objectMapper.readValue(
                decodeHtmlEntities(response.body()), NotificationRegisterResponse.class);

        return notificationRegisterResponse.getValue();
    }

    private void removeJolokiaNotification(final UUID nodeID, final String notificationID) throws UnknownHostException
    {
        String url = mountJolokiaBaseURL(nodeID) + "/notification";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                .POST(HttpRequest.BodyPublishers.ofString(
                        jolokiaRemoveNotificationOptions(nodeID, notificationID)))
                .build();
        try
        {
            buildHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException | InterruptedException e)
        {
            LOG.error("Error trying to remove NotificationListener with ID {} in node {}",
                    notificationID, nodeID, e);
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
            LOG.error("Unable to serialize notification options for node {}", nodeID, e);
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
            LOG.error("Unable to serialize Jolokia Notification Options for node {}", nodeID,
                    e);
        }
        return "";
    }

    private String mountJolokiaBaseURL(final UUID nodeID) throws UnknownHostException
    {
        String host = myNativeConnectionProvider.getNodes().get(nodeID).getBroadcastRpcAddress().get().getAddress().getHostAddress();
        if (NO_BROADCAST_ADDRESS.equals(host))
        {
            host = myNativeConnectionProvider.getNodes().get(nodeID).getListenAddress().get().getHostString();
        }
        if (myIpTranslator.isActive())
        {
            host = myIpTranslator.getInternalIp(host);
        }
        if (myReverseDNSResolution)
        {
            host = ReverseDNS.fromHostString(host);
        }
        if (host.contains(":") && !host.startsWith("[") && !host.endsWith("]"))
        {
            // Use square brackets to surround IPv6 addresses
            host = "[" + host + "]";
        }
        return myURLPrefix + "://" + host + ":" + myJolokiaPort + "/jolokia";
    }

    private String checkForNotificationsWithRetry(final UUID nodeID, final String notificationID)
            throws IOException, InterruptedException
    {
        IOException lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++)
        {
            try
            {
                return checkForNotifications(nodeID, notificationID);
            }
            catch (IOException e)
            {
                lastException = e;
                LOG.debug("Notification check attempt {}/{} failed for node {}",
                        attempt, MAX_RETRIES, nodeID, e);
                if (attempt < MAX_RETRIES)
                {
                    long delay = INITIAL_RETRY_DELAY_IN_MS * (1L << (attempt - 1));
                    Thread.sleep(delay);
                }
            }
        }
        // All retries failed — attempt to re-register client and try once more
        LOG.debug("All {} retries failed for node {}, attempting client re-registration", MAX_RETRIES, nodeID);
        try
        {
            registerClientId(nodeID);
            return checkForNotifications(nodeID, notificationID);
        }
        catch (IOException e)
        {
            LOG.warn("Re-registration attempt also failed for node {}", nodeID, e);
        }
        throw lastException;
    }

    private String checkForNotifications(final UUID nodeID, final String notificationID)
            throws IOException, InterruptedException
    {
        synchronized (myClientIdMap)
        {
            Map<String, String> clientInfo = myClientIdMap.get(nodeID);
            if (clientInfo == null)
            {
                LOG.debug("No client info found for node {}, skipping notification check", nodeID);
                return "{}";
            }

            String url = mountJolokiaBaseURL(nodeID) + "/exec/" + clientInfo.get("store") + "/pull/"
                + clientInfo.get(CLIENT_ID_PROPERTY) + "/" + notificationID;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                    .GET()
                    .build();

            HttpResponse<String> response = buildHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            return decodeHtmlEntities(response.body());
        }
    }

    private String decodeHtmlEntities(final String input)
    {
        return input
                .replace("&quot;", "\"")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&");
    }

    public final void close()
    {
        // Cancel all notification monitors
        synchronized (myNotificationMonitors)
        {
            for (Map<String, ScheduledFuture<?>> nodeMonitors : myNotificationMonitors.values())
            {
                for (ScheduledFuture<?> future : nodeMonitors.values())
                {
                    future.cancel(true);
                }
            }
            myNotificationMonitors.clear();
        }

        // Shutdown executor
        myNotificationExecutor.shutdown();
        try
        {
            if (!myNotificationExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                myNotificationExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            myNotificationExecutor.shutdownNow();
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        public static final int DEFAULT_RUN_DELAY = 500;
        private static final int DEFAULT_JOLOKIA_PORT = 8778;

        private DistributedNativeConnectionProvider myNativeConnection;
        private int myJolokiaPort = DEFAULT_JOLOKIA_PORT;
        private boolean myJolokiaPEM = false;
        private boolean myReverseDNSResolution = false;
        private long myRunDelay = DEFAULT_RUN_DELAY;
        private IpTranslator myIpTranslator;
        private CertificateHandler myCertificateHandler;

        /**
         * Sets the native connection provider used by the controller.
         *
         * @param nativeConnectionProvider
         *         provider responsible for native connections
         * @return Builder
         */
        public Builder withNativeConnection(final DistributedNativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnection = nativeConnectionProvider;
            return this;
        }

        /**
         * Sets the Jolokia port used for connections.
         *
         * @param jolokiaPort
         *         port number for Jolokia endpoint
         * @return Builder
         */
        public Builder withJolokiaPort(final int jolokiaPort)
        {
            myJolokiaPort = jolokiaPort;
            return this;
        }

        /**
         * Enables or disables the use of Jolokia PEM configuration.
         *
         * @param jolokiaPEM
         *         true to enable PEM, false otherwise
         * @return Builder
         */
        public Builder withJolokiaPEM(final boolean jolokiaPEM)
        {
            myJolokiaPEM = jolokiaPEM;
            return this;
        }

        /**
         * Enables or disables reverse DNS resolution.
         *
         * @param reverseDNSResolution
         *         true to enable reverse DNS resolution, false otherwise
         * @return Builder
         */
        public Builder withReverseDNSResolution(final boolean reverseDNSResolution)
        {
            myReverseDNSResolution = reverseDNSResolution;
            return this;
        }

        /**
         * Sets the delay between controller runs.
         *
         * @param runDelay
         *         delay in milliseconds
         * @return Builder
         */
        public Builder withRunDelay(final long runDelay)
        {
            myRunDelay = runDelay;
            return this;
        }

        /**
         * Sets the IP translator used to resolve node addresses.
         *
         * @param ipTranslator
         *         translator implementation
         * @return Builder
         */
        public Builder withIpTranslator(final IpTranslator ipTranslator)
        {
            myIpTranslator = ipTranslator;
            return this;
        }

        /**
         * Sets the certificate handler used for secure connections.
         *
         * @param certificateHandler
         *         handler responsible for certificate management
         * @return Builder
         */
        public Builder withCertificateHandler(final CertificateHandler certificateHandler)
        {
            myCertificateHandler = certificateHandler;
            return this;
        }

        /**
         * Builds a {@link JolokiaNotificationController} instance using the configured parameters.
         *
         * @return JolokiaNotificationController
         */
        public JolokiaNotificationController build()
        {
            return new JolokiaNotificationController(this);
        }
    }
}
