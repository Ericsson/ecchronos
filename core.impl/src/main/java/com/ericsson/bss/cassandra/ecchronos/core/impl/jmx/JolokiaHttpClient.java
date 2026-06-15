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

import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.ClientRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.http.NotificationRegisterResponse;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.google.common.annotations.VisibleForTesting;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

final class JolokiaHttpClient implements java.io.Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(JolokiaHttpClient.class);
    private static final int HTTP_TIMEOUT_SECONDS = 5;
    private static final int HTTP_REQUEST_TIMEOUT_SECONDS = 10;
    private static final int SSL_SESSION_CACHE_SIZE = 50;
    private static final int SSL_SESSION_TIMEOUT_SECONDS = 3600;
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_IN_MS = 500;
    private static final int HTTP_EXECUTOR_POOL_SIZE = 8;
    private static final String CLIENT_ID_PROPERTY = "clientID";
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    public static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP

    private final Map<UUID, Map<String, String>> myClientIdMap = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CertificateHandler myCertificateHandler;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final int myJolokiaPort;
    private final boolean myReverseDNSResolution;
    private final String myURLPrefix;
    private final IpTranslator myIpTranslator;
    private volatile HttpClient myHttpClient;
    private volatile SSLContext myCurrentSSLContext;
    private volatile ExecutorService myCurrentExecutor;

    JolokiaHttpClient(final CertificateHandler certificateHandler,
                      final DistributedNativeConnectionProvider nativeConnectionProvider,
                      final int jolokiaPort,
                      final boolean jolokiaPEM,
                      final boolean reverseDNSResolution,
                      final IpTranslator ipTranslator)
    {
        myCertificateHandler = certificateHandler;
        myNativeConnectionProvider = nativeConnectionProvider;
        myJolokiaPort = jolokiaPort;
        myURLPrefix = jolokiaPEM ? "https" : "http";
        myReverseDNSResolution = reverseDNSResolution;
        myIpTranslator = ipTranslator;
        myHttpClient = buildHttpClient();
    }

    @VisibleForTesting
    HttpClient getHttpClient()
    {
        if (myCertificateHandler != null)
        {
            SSLContext latestContext = myCertificateHandler.getSSLContext();
            if (latestContext != null && latestContext != myCurrentSSLContext) // NOPMD CompareObjectsWithEquals
            {
                ExecutorService oldExecutor = myCurrentExecutor;
                myHttpClient = buildHttpClient();
                if (oldExecutor != null)
                {
                    oldExecutor.shutdownNow();
                    LOG.info("Previous HttpClient executor shut down after certificate rotation");
                }
            }
        }
        return myHttpClient;
    }

    private HttpClient buildHttpClient()
    {
        ExecutorService executor = Executors.newFixedThreadPool(HTTP_EXECUTOR_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("JolokiaHttp-%d").setDaemon(true).build());
        myCurrentExecutor = executor;
        HttpClient.Builder builder = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(java.time.Duration.ofSeconds(HTTP_TIMEOUT_SECONDS));
        if (myCertificateHandler != null)
        {
            SSLContext sslContext = myCertificateHandler.getSSLContext();
            if (sslContext != null)
            {
                sslContext.getClientSessionContext().setSessionCacheSize(SSL_SESSION_CACHE_SIZE);
                sslContext.getClientSessionContext().setSessionTimeout(SSL_SESSION_TIMEOUT_SECONDS);
                builder.sslContext(sslContext);
                myCurrentSSLContext = sslContext;
            }
        }
        return builder.build();
    }

    public String getClientId(final UUID nodeID)
    {
        Map<String, String> clientInfo = myClientIdMap.get(nodeID);
        return clientInfo != null ? clientInfo.get(CLIENT_ID_PROPERTY) : null;
    }

    public void registerClientId(final UUID nodeID) throws IOException, InterruptedException
    {
        String url = mountJolokiaBaseURL(nodeID) + "/notification/register";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                .GET()
                .build();

        HttpResponse<String> response = getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        LOG.debug("Raw response from {}: Status={}, Body={}", url, response.statusCode(), response.body());

        ClientRegisterResponse clientRegisterResponse = objectMapper.readValue(
                decodeHtmlEntities(response.body()), ClientRegisterResponse.class);

        Map<String, String> properties = new HashMap<>();
        properties.put(CLIENT_ID_PROPERTY, clientRegisterResponse.getValue().getId());
        properties.put("store", clientRegisterResponse.getValue().getBackend().getPull().getStore());

        myClientIdMap.put(nodeID, properties);
    }

    public String registerJolokiaNotification(final UUID nodeID) throws IOException, InterruptedException
    {
        String url = mountJolokiaBaseURL(nodeID) + "/notification";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(java.time.Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SECONDS))
                .POST(HttpRequest.BodyPublishers.ofString(jolokiaCreateNotificationOptions(nodeID)))
                .build();
        HttpResponse<String> response = getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

        NotificationRegisterResponse notificationRegisterResponse = objectMapper.readValue(
                decodeHtmlEntities(response.body()), NotificationRegisterResponse.class);

        return notificationRegisterResponse.getValue();
    }

    public void removeJolokiaNotification(final UUID nodeID, final String notificationID) throws UnknownHostException
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
            getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException | InterruptedException e)
        {
            LOG.error("Error trying to remove NotificationListener with ID {} in node {}",
                    notificationID, nodeID, e);
        }
    }

    public String checkForNotificationsWithRetry(final UUID nodeID, final String notificationID)
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

        HttpResponse<String> response = getHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        return decodeHtmlEntities(response.body());
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
            host = "[" + host + "]";
        }
        return myURLPrefix + "://" + host + ":" + myJolokiaPort + "/jolokia";
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
            LOG.error("Unable to serialize Jolokia Notification Options for node {}", nodeID, e);
        }
        return "";
    }

    private String decodeHtmlEntities(final String input)
    {
        return input
                .replace("&quot;", "\"")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&");
    }

    @Override
    public void close()
    {
        if (myCurrentExecutor != null)
        {
            myCurrentExecutor.shutdownNow();
        }
    }
}
