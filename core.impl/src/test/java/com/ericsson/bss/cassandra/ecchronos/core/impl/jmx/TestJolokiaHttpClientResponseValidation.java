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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationListenerResponse;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for JolokiaHttpClient response validation (Bug 1) and retry with JacksonException (Bug 2).
 * Uses a local JDK HttpServer to simulate Jolokia responses.
 */
public class TestJolokiaHttpClientResponseValidation
{
    private HttpServer httpServer;
    private int httpPort;
    private JolokiaHttpClient jolokiaHttpClient;
    private DistributedNativeConnectionProvider mockNativeProvider;
    private final UUID nodeID = UUID.randomUUID();
    private final String notificationID = "test-notification-1";

    @Before
    public void setup() throws IOException
    {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpPort = httpServer.getAddress().getPort();

        mockNativeProvider = mock(DistributedNativeConnectionProvider.class);
        Node mockNode = mock(Node.class);
        IpTranslator ipTranslator = mock(IpTranslator.class);

        InetSocketAddress broadcastAddr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(broadcastAddr));
        when(mockNativeProvider.getNodes()).thenReturn(Map.of(nodeID, mockNode));

        jolokiaHttpClient = new JolokiaHttpClient(null, mockNativeProvider, httpPort, false, false, ipTranslator);

        // Register client ID manually by simulating a register endpoint
        httpServer.createContext("/jolokia/notification/register", exchange ->
        {
            String body = "{\"value\":{\"id\":\"client-1\",\"backend\":{\"pull\":{\"store\":\"myStore\"}}}}";
            exchange.sendResponseHeaders(200, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        httpServer.start();

        // Register the client
        try
        {
            jolokiaHttpClient.registerClientId(nodeID);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    @After
    public void teardown()
    {
        if (httpServer != null)
        {
            httpServer.stop(0);
        }
        if (jolokiaHttpClient != null)
        {
            jolokiaHttpClient.close();
        }
    }

    @Test
    public void testEmptyBodyThrowsIOExceptionAndRetriesExhausted()
    {
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        });

        assertThatThrownBy(() -> jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty response body");
    }

    @Test
    public void testNon200StatusThrowsIOExceptionAndRetriesExhausted()
    {
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            String body = "Service Unavailable";
            exchange.sendResponseHeaders(503, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        assertThatThrownBy(() -> jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("HTTP status 503");
    }

    @Test
    public void testMalformedJsonRetriesAndFails()
    {
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            String body = "not-valid-json{{{";
            exchange.sendResponseHeaders(200, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        assertThatThrownBy(() -> jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to parse");
    }

    @Test
    public void testValidResponseReturnsParsedObject() throws IOException, InterruptedException
    {
        String validJson = "{\"request\":{},\"value\":{\"notifications\":[]},\"status\":200,\"timestamp\":1234567890}";
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            exchange.sendResponseHeaders(200, validJson.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(validJson.getBytes());
            }
        });

        NotificationListenerResponse response =
                jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    public void testTransientFailureThenSuccessReturnsValidResponse() throws IOException, InterruptedException
    {
        AtomicInteger callCount = new AtomicInteger(0);
        String validJson = "{\"request\":{},\"value\":{\"notifications\":[]},\"status\":200,\"timestamp\":1234567890}";
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            int attempt = callCount.incrementAndGet();
            if (attempt <= 2)
            {
                // First 2 attempts return empty body
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().close();
            }
            else
            {
                // Third attempt returns valid response
                exchange.sendResponseHeaders(200, validJson.length());
                try (OutputStream os = exchange.getResponseBody())
                {
                    os.write(validJson.getBytes());
                }
            }
        });

        NotificationListenerResponse response =
                jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(callCount.get()).isEqualTo(3);
    }

    @Test
    public void testBlankBodyThrowsIOException()
    {
        httpServer.createContext("/jolokia/exec/myStore/pull/client-1/" + notificationID, exchange ->
        {
            String body = "   ";
            exchange.sendResponseHeaders(200, body.length());
            try (OutputStream os = exchange.getResponseBody())
            {
                os.write(body.getBytes());
            }
        });

        assertThatThrownBy(() -> jolokiaHttpClient.checkForNotificationsWithRetry(nodeID, notificationID))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty response body");
    }
}
