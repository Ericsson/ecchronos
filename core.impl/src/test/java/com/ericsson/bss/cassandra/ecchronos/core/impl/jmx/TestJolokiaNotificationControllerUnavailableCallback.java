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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.metadata.Node;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.Notification;
import javax.management.NotificationListener;

/**
 * Tests that the nodeUnavailableCallback is invoked after consecutive
 * Jolokia notification polling failures.
 */
public class TestJolokiaNotificationControllerUnavailableCallback
{
    private HttpServer httpServer;
    private JolokiaNotificationController controller;
    private final UUID nodeID = UUID.randomUUID();
    private final CopyOnWriteArrayList<UUID> unavailableNodes = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Notification> receivedNotifications = new CopyOnWriteArrayList<>();

    @Before
    public void setup() throws IOException, InterruptedException
    {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        int httpPort = httpServer.getAddress().getPort();

        DistributedNativeConnectionProvider mockNativeProvider = mock(DistributedNativeConnectionProvider.class);
        Node mockNode = mock(Node.class);
        IpTranslator ipTranslator = mock(IpTranslator.class);

        InetSocketAddress broadcastAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(broadcastAddr));
        when(mockNativeProvider.getNodes()).thenReturn(Map.of(nodeID, mockNode));

        // Register client endpoint
        httpServer.createContext("/jolokia/notification/register", exchange ->
        {
            String body = "{\"value\":{\"id\":\"client-1\",\"backend\":{\"pull\":{\"store\":\"myStore\"}}}}";
            exchange.sendResponseHeaders(200, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        // Notification registration endpoint
        httpServer.createContext("/jolokia/notification", exchange ->
        {
            String body = "{\"value\":\"notif-1\"}";
            exchange.sendResponseHeaders(200, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        // Notification polling endpoint — always returns empty body to trigger failures
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/notif-1", exchange ->
        {
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });

        httpServer.start();

        controller = JolokiaNotificationController.newBuilder()
                .withNativeConnection(mockNativeProvider)
                .withJolokiaPort(httpPort)
                .withJolokiaPEM(false)
                .withReverseDNSResolution(false)
                .withRunDelay(100)
                .withIpTranslator(ipTranslator)
                .withNodeUnavailableCallback(unavailableNodes::add)
                .build();
    }

    @After
    public void teardown()
    {
        if (controller != null)
        {
            controller.close();
        }
        if (httpServer != null)
        {
            httpServer.stop(0);
        }
    }

    @Test
    public void testCallbackInvokedAfterConsecutiveFailures() throws IOException, InterruptedException
    {
        NotificationListener listener = (notification, handback) -> receivedNotifications.add(notification);

        controller.addStorageServiceListener(nodeID, listener);

        // The poller runs every 100ms and the endpoint always returns empty body.
        // After 5 consecutive failures (MAX_CONSECUTIVE_FAILURES), the callback should fire.
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(unavailableNodes).contains(nodeID));
    }

    @Test
    public void testConnectionFailedNotificationSentToListener() throws IOException, InterruptedException
    {
        NotificationListener listener = (notification, handback) -> receivedNotifications.add(notification);

        controller.addStorageServiceListener(nodeID, listener);

        // Verify the listener receives a jmx.remote.connection.failed notification
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                {
                    assertThat(receivedNotifications)
                            .anyMatch(n -> "jmx.remote.connection.failed".equals(n.getType()));
                });
    }

    @Test
    public void testNoCallbackWhenNotConfigured() throws IOException, InterruptedException
    {
        // Build a controller without a callback
        DistributedNativeConnectionProvider mockNativeProvider = mock(DistributedNativeConnectionProvider.class);
        Node mockNode = mock(Node.class);
        IpTranslator ipTranslator = mock(IpTranslator.class);
        InetSocketAddress broadcastAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(broadcastAddr));
        when(mockNativeProvider.getNodes()).thenReturn(Map.of(nodeID, mockNode));

        JolokiaNotificationController controllerNoCallback = JolokiaNotificationController.newBuilder()
                .withNativeConnection(mockNativeProvider)
                .withJolokiaPort(httpServer.getAddress().getPort())
                .withJolokiaPEM(false)
                .withReverseDNSResolution(false)
                .withRunDelay(100)
                .withIpTranslator(ipTranslator)
                .build();

        try
        {
            NotificationListener listener = (notification, handback) -> receivedNotifications.add(notification);
            controllerNoCallback.addStorageServiceListener(nodeID, listener);

            // Wait for failure notification to be sent (proves failures happened)
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() ->
                    {
                        assertThat(receivedNotifications)
                                .anyMatch(n -> "jmx.remote.connection.failed".equals(n.getType()));
                    });

            // No callback was configured, so unavailableNodes should remain empty
            assertThat(unavailableNodes).isEmpty();
        }
        finally
        {
            controllerNoCallback.close();
        }
    }
}
