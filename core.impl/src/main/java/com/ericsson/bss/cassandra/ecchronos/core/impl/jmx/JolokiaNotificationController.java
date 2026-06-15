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

import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationListenerResponse;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class JolokiaNotificationController implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(JolokiaNotificationController.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 1;
    private static final int MAX_CONSECUTIVE_FAILURES = 5;

    private static final int NOTIFICATION_THREAD_POOL_SIZE = 4;

    private final Map<NotificationListener, String> myJolokiaRelationshipListeners = new HashMap<>();

    private final ScheduledExecutorService myNotificationExecutor = Executors.newScheduledThreadPool(
            NOTIFICATION_THREAD_POOL_SIZE,
            new ThreadFactoryBuilder().setNameFormat("NotificationRefresher-%d").build());

    private final Map<UUID, Map<String, NotificationListener>> myNodeListenersMap = new ConcurrentHashMap<>();
    private final Map<UUID, Map<String, ScheduledFuture<?>>> myNotificationMonitors = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<UUID, ReentrantLock> myNodeLocks = new ConcurrentHashMap<>();
    private final Semaphore myPollingSemaphore;

    private final JolokiaHttpClient myJolokiaHttpClient;
    private final long myRunDelay;

    public JolokiaNotificationController(final Builder builder)
    {
        myRunDelay = builder.myRunDelay;
        myPollingSemaphore = new Semaphore(NOTIFICATION_THREAD_POOL_SIZE);
        myJolokiaHttpClient = new JolokiaHttpClient(
                builder.myCertificateHandler,
                builder.myNativeConnection,
                builder.myJolokiaPort,
                builder.myJolokiaPEM,
                builder.myReverseDNSResolution,
                builder.myIpTranslator
        );
    }

    private ReentrantLock getNodeLock(final UUID nodeID)
    {
        return myNodeLocks.computeIfAbsent(nodeID, id -> new ReentrantLock());
    }

    public final void addStorageServiceListener(final UUID nodeID, final NotificationListener listener) throws IOException, InterruptedException
    {
        myJolokiaHttpClient.registerClientId(nodeID);

        String jolokiaNotificationID = myJolokiaHttpClient.registerJolokiaNotification(nodeID);

        synchronized (myNodeListenersMap)
        {
            myNodeListenersMap.computeIfAbsent(nodeID, k -> new ConcurrentHashMap<>()).put(jolokiaNotificationID, listener);
        }

        myJolokiaRelationshipListeners.put(listener, jolokiaNotificationID);

        try
        {
            startNotificationMonitor(nodeID, jolokiaNotificationID);
        }
        catch (Exception e)
        {
            myJolokiaRelationshipListeners.remove(listener);
            synchronized (myNodeListenersMap)
            {
                Map<String, NotificationListener> listeners = myNodeListenersMap.get(nodeID);
                if (listeners != null)
                {
                    listeners.remove(jolokiaNotificationID);
                }
            }
            try
            {
                myJolokiaHttpClient.removeJolokiaNotification(nodeID, jolokiaNotificationID);
            }
            catch (Exception removeEx)
            {
                LOG.warn("Failed to remove Jolokia notification during cleanup for node {}", nodeID, removeEx);
            }
            throw new IOException("Failed to start notification monitor for node " + nodeID, e);
        }
    }

    public final void removeStorageServiceListener(
            final UUID nodeID,
            final NotificationListener listener) throws UnknownHostException
    {
        String jolokiaNotificationID = myJolokiaRelationshipListeners.get(listener);
        if (jolokiaNotificationID == null)
        {
            LOG.warn("No Jolokia notification ID found for listener on node {}, skipping removal", nodeID);
            return;
        }
        myJolokiaHttpClient.removeJolokiaNotification(nodeID, jolokiaNotificationID);
        synchronized (myNotificationMonitors)
        {
            Map<String, ScheduledFuture<?>> monitors = myNotificationMonitors.get(nodeID);
            if (monitors != null)
            {
                ScheduledFuture<?> future = monitors.remove(jolokiaNotificationID);
                if (future != null)
                {
                    future.cancel(true);
                }
            }
        }
        Map<String, NotificationListener> listeners = myNodeListenersMap.get(nodeID);
        if (listeners != null)
        {
            listeners.remove(jolokiaNotificationID);
        }
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
        private static final int MAX_NOTIFICATION_HISTORY = 1000;
        private final UUID myNodeID;
        private final String myNotificationID;
        private final Set<Integer> notificationController = new LinkedHashSet<>()
        {
            @Override
            public boolean add(final Integer e)
            {
                if (size() >= MAX_NOTIFICATION_HISTORY)
                {
                    Iterator<Integer> it = iterator();
                    it.next();
                    it.remove();
                }
                return super.add(e);
            }
        };
        private int consecutiveFailures = 0;

        private NotificationRunTask(final UUID nodeID, final String notificationID)
        {
            myNodeID = nodeID;
            myNotificationID = notificationID;
        }

        @Override
        public void run()
        {
            if (!myPollingSemaphore.tryAcquire())
            {
                LOG.debug("Polling concurrency limit reached, skipping poll for node {} notificationID {}",
                        myNodeID, myNotificationID);
                return;
            }
            try
            {
                ReentrantLock lock = getNodeLock(myNodeID);
                lock.lock();
                try
                {
                    String response = myJolokiaHttpClient.checkForNotificationsWithRetry(myNodeID, myNotificationID);
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
                finally
                {
                    lock.unlock();
                }
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
            finally
            {
                myPollingSemaphore.release();
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

    @Override
    public final void close()
    {
        // Remove remote Jolokia notifications
        for (Map.Entry<UUID, Map<String, ScheduledFuture<?>>> nodeEntry : myNotificationMonitors.entrySet())
        {
            UUID nodeID = nodeEntry.getKey();
            for (String notificationID : nodeEntry.getValue().keySet())
            {
                try
                {
                    myJolokiaHttpClient.removeJolokiaNotification(nodeID, notificationID);
                }
                catch (Exception e)
                {
                    LOG.debug("Failed to remove remote Jolokia notification {} for node {}", notificationID, nodeID, e);
                }
            }
        }

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

        myNodeListenersMap.clear();
        myJolokiaRelationshipListeners.clear();
        myNodeLocks.clear();

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
        myJolokiaHttpClient.close();
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
