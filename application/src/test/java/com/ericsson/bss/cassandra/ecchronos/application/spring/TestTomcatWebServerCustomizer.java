/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.utils.CertUtils;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.*;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.awaitility.Durations;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.Duration;
import java.util.Date;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

public abstract class TestTomcatWebServerCustomizer
{
    protected static final String SERVER_COMMON_NAME = "server";
    protected static final String SERVER_CA_COMMON_NAME = "serverCA";
    protected static final String METRICS_SERVER_COMMON_NAME = "metricsServer";
    protected static final String METRICS_SERVER_CA_COMMON_NAME = "metricsServerCA";
    protected static final String CLIENT_CA_COMMON_NAME = "clientCA";
    protected static final String CLIENT_COMMON_NAME = "client";
    protected static final String EXPIRED_CLIENT_COMMON_NAME = "expiredClient";
    protected static final String METRICS_CLIENT_CA_COMMON_NAME = "metricsClientCA";
    protected static final String METRICS_CLIENT_COMMON_NAME = "metricsClient";
    protected static final String EXPIRED_METRICS_CLIENT_COMMON_NAME = "expiredMetricsClient";
    protected static final String KEYSTORE_PASSWORD = "ecctest";

    protected static final int REFRESH_RATE_IN_MS = 100;
    protected static final int METRICS_REFRESH_RATE_IN_MS = 50;
    protected static final int INVOCATION_COUNT = 1;

    private static final Duration RELOAD_TIMEOUT = Durations.TEN_SECONDS;

    // These must be set in @BeforeClass
    protected static String serverCaCert;
    protected static String serverCaCertKey;
    protected static String serverCert;
    protected static String serverCertKey;
    protected static String serverKeyStore;
    protected static String serverTrustStore;
    protected static String metricsServerCaCert;
    protected static String metricsServerCaCertKey;
    protected static String metricsServerCert;
    protected static String metricsServerCertKey;
    protected static String metricsServerKeyStore;
    protected static String metricsServerTrustStore;

    protected static String clientCaCert;
    protected static String clientCaCertKey;
    protected static String clientCert;
    protected static String clientCertKey;
    protected static String clientKeyStore;
    protected static String clientTrustStore;
    protected static String expiredClientCert;
    protected static String expiredClientCertKey;
    protected static String expiredClientKeyStore;
    protected static String metricsClientCaCert;
    protected static String metricsClientCaCertKey;
    protected static String metricsClientCert;
    protected static String metricsClientCertKey;
    protected static String metricsClientKeyStore;
    protected static String metricsClientTrustStore;
    protected static String expiredMetricsClientCert;
    protected static String expiredMetricsClientCertKey;
    protected static String expiredMetricsClientKeyStore;
    private static CertUtils certUtils = new CertUtils();

    private String httpsUrl;
    private String metricsServerHttpsUrl;

    @Autowired
    private Environment environment;

    @MockitoBean
    private ECChronos ecChronos;

    @MockitoBean
    private TableReferenceFactory tableReferenceFactory;

    @MockitoBean
    private ReplicatedTableProvider replicatedTableProvider;

    @MockitoBean
    private RepairStatsProvider repairStatsProvider;

    @MockitoBean
    private RepairScheduler repairScheduler;

    @MockitoBean
    private OnDemandRepairScheduler onDemandRepairScheduler;

    @MockitoBean
    private NativeConnectionProvider nativeConnectionProvider;

    @MockitoBean
    private JmxConnectionProvider jmxConnectionProvider;

    @MockitoBean
    private ReplicationState replicationState;

    @MockitoBean
    private NodeResolver nodeResolver;

    @MockitoBean
    private RepairHistoryBean repairHistoryBean;

    @MockitoBean
    private CassandraHealthIndicator cassandraHealthIndicator;

    @MockitoBean
    private TimeBasedRunPolicy timeBasedRunPolicy;

    @MockitoSpyBean
    protected TomcatWebServerCustomizer tomcatWebServerCustomizer;

    @BeforeClass
    public static void initOnce() throws IOException
    {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        setupPathsForCerts(temporaryFolder.getRoot().getPath());
    }

    private static void setupPathsForCerts(String directory)
    {
        serverCaCert = Paths.get(directory, "serverca.crt").toString();
        serverCaCertKey = Paths.get(directory, "serverca.key").toString();
        serverCert = Paths.get(directory, "server.crt").toString();
        serverCertKey = Paths.get(directory, "server.key").toString();
        serverKeyStore = Paths.get(directory, "server.keystore").toString();
        serverTrustStore = Paths.get(directory, "server.truststore").toString();
        metricsServerCaCert = Paths.get(directory, "metricsserverca.crt").toString();
        metricsServerCaCertKey = Paths.get(directory, "metricsserverca.key").toString();
        metricsServerCert = Paths.get(directory, "metricsserver.crt").toString();
        metricsServerCertKey = Paths.get(directory, "metricsserver.key").toString();
        metricsServerKeyStore = Paths.get(directory, "metricsserver.keystore").toString();
        metricsServerTrustStore = Paths.get(directory, "metricsserver.truststore").toString();

        clientCaCert = Paths.get(directory, "clientca.crt").toString();
        clientCaCertKey = Paths.get(directory, "clientca.key").toString();
        clientCert = Paths.get(directory, "client.crt").toString();
        clientCertKey = Paths.get(directory, "client.key").toString();
        clientKeyStore = Paths.get(directory, "client.keystore").toString();
        clientTrustStore = Paths.get(directory, "client.truststore").toString();
        expiredClientCert = Paths.get(directory, "expired-client.crt").toString();
        expiredClientCertKey = Paths.get(directory, "expired-client.key").toString();
        expiredClientKeyStore = Paths.get(directory, "expired-client.keystore").toString();
        metricsClientCaCert = Paths.get(directory, "metricsclientca.crt").toString();
        metricsClientCaCertKey = Paths.get(directory, "metricsclientca.key").toString();
        metricsClientCert = Paths.get(directory, "metricsclient.crt").toString();
        metricsClientCertKey = Paths.get(directory, "metricsclient.key").toString();
        metricsClientKeyStore = Paths.get(directory, "metricsclient.keystore").toString();
        metricsClientTrustStore = Paths.get(directory, "metricsclient.truststore").toString();
        expiredMetricsClientCert = Paths.get(directory, "expired-metricsclient.crt").toString();
        expiredMetricsClientCertKey = Paths.get(directory, "expired-metricsclient.key").toString();
        expiredMetricsClientKeyStore = Paths.get(directory, "expired-metricsclient.keystore").toString();
    }

    public static void createCerts(String algorithm, boolean isServerUsingKeyStores)
    {
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plus(java.time.Duration.ofHours(1)));

        // Server
        certUtils.createSelfSignedCACertificate(SERVER_CA_COMMON_NAME, notBefore, notAfter, algorithm, serverCaCert, serverCaCertKey);
        certUtils.createCertificate(SERVER_COMMON_NAME, notBefore, notAfter, serverCaCert, serverCaCertKey, serverCert,
                serverCertKey);
        certUtils.createSelfSignedCACertificate(CLIENT_CA_COMMON_NAME, notBefore, notAfter, algorithm, clientCaCert, clientCaCertKey);
        certUtils.createCertificate(CLIENT_COMMON_NAME, notBefore, notAfter, clientCaCert, clientCaCertKey, clientCert,
                clientCertKey);
        certUtils.createKeyStore(clientCert, clientCertKey, "PKCS12", KEYSTORE_PASSWORD, clientKeyStore);
        certUtils.createCertificate(EXPIRED_CLIENT_COMMON_NAME, notBefore, notBefore, clientCaCert, clientCaCertKey,
                expiredClientCert, expiredClientCertKey);
        certUtils.createKeyStore(expiredClientCert, expiredClientCertKey, "PKCS12", KEYSTORE_PASSWORD,
                expiredClientKeyStore);
        certUtils.createTrustStore(serverCaCert, "PKCS12", KEYSTORE_PASSWORD, clientTrustStore);

        // Metrics server
        certUtils.createSelfSignedCACertificate(METRICS_SERVER_CA_COMMON_NAME, notBefore, notAfter, algorithm, metricsServerCaCert, metricsServerCaCertKey);
        certUtils.createCertificate(METRICS_SERVER_COMMON_NAME, notBefore, notAfter, metricsServerCaCert, metricsServerCaCertKey, metricsServerCert,
                metricsServerCertKey);

        certUtils.createSelfSignedCACertificate(METRICS_CLIENT_CA_COMMON_NAME, notBefore, notAfter, algorithm, metricsClientCaCert, metricsClientCaCertKey);
        certUtils.createCertificate(METRICS_CLIENT_COMMON_NAME, notBefore, notAfter, metricsClientCaCert, metricsClientCaCertKey, metricsClientCert,
                metricsClientCertKey);
        certUtils.createKeyStore(metricsClientCert, metricsClientCertKey, "PKCS12", KEYSTORE_PASSWORD, metricsClientKeyStore);
        certUtils.createCertificate(EXPIRED_METRICS_CLIENT_COMMON_NAME, notBefore, notBefore, metricsClientCaCert, metricsClientCaCertKey,
                expiredMetricsClientCert, expiredMetricsClientCertKey);
        certUtils.createKeyStore(expiredMetricsClientCert, expiredMetricsClientCertKey, "PKCS12", KEYSTORE_PASSWORD,
                expiredMetricsClientKeyStore);
        certUtils.createTrustStore(metricsServerCaCert, "PKCS12", KEYSTORE_PASSWORD, metricsClientTrustStore);

        if (isServerUsingKeyStores)
        {
            certUtils.createKeyStore(serverCert, serverCertKey, "PKCS12", KEYSTORE_PASSWORD, serverKeyStore);
            certUtils.createKeyStore(metricsServerCert, metricsServerCertKey, "PKCS12", KEYSTORE_PASSWORD, metricsServerKeyStore);
            certUtils.createTrustStore(clientCaCert, "PKCS12", KEYSTORE_PASSWORD, serverTrustStore);
            certUtils.createTrustStore(metricsClientCaCert, "PKCS12", KEYSTORE_PASSWORD, metricsServerTrustStore);
        }
    }

    @Before
    public void init()
    {
        String httpsPort = environment.getProperty("local.server.port");
        httpsUrl = "https://localhost:" + httpsPort + "/actuator/health";

        tomcatWebServerCustomizer.setMetricsPortProperty();
        String metricsHttpsPort = environment.getProperty("metricsServer.local.port");
        metricsServerHttpsUrl = "https://localhost:" + metricsHttpsPort + "/actuator/health";
    }

    @Test
    public void testSuccessfulCertificateReloading()
    {
        await().atMost(RELOAD_TIMEOUT)
                .untilAsserted(() -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadSslContext());
        await().atMost(RELOAD_TIMEOUT)
                .untilAsserted(() -> verify(tomcatWebServerCustomizer,
                        atLeast(INVOCATION_COUNT)).reloadMetricsServerSslContext());
    }

    @Test
    public void testSuccessfulResponseWhenValidCertificate() throws IOException, GeneralSecurityException
    {
        HttpResponse response = configureHttpClient(clientKeyStore, clientTrustStore, KEYSTORE_PASSWORD).execute(new HttpGet(httpsUrl));
        assertThat(response.getCode()).isEqualTo(HTTP_OK);
        HttpResponse metricsResponse = configureHttpClient(metricsClientKeyStore, metricsClientTrustStore,
                KEYSTORE_PASSWORD).execute(new HttpGet(metricsServerHttpsUrl));
        assertThat(metricsResponse.getCode()).isEqualTo(HTTP_OK);
    }

    @Test
    public void testExceptionWhenExpiredCertificate() throws IOException, GeneralSecurityException
    {
        HttpClient httpClient = configureHttpClient(expiredClientKeyStore, clientTrustStore, KEYSTORE_PASSWORD);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> httpClient.execute(new HttpGet(httpsUrl)));
        HttpClient metricsHttpClient = configureHttpClient(expiredMetricsClientKeyStore, metricsClientTrustStore, KEYSTORE_PASSWORD);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> metricsHttpClient.execute(new HttpGet(metricsServerHttpsUrl)));
    }

    @Test
    public void testExceptionWhenCertificateForWrongServer() throws IOException, GeneralSecurityException
    {
        HttpClient httpClient = configureHttpClient(clientKeyStore, clientTrustStore, KEYSTORE_PASSWORD);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> httpClient.execute(new HttpGet(metricsServerHttpsUrl)));
        HttpClient metricsHttpClient = configureHttpClient(metricsClientKeyStore, metricsClientTrustStore,
                KEYSTORE_PASSWORD);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> metricsHttpClient.execute(new HttpGet(httpsUrl)));
    }

    private HttpClient configureHttpClient(String keyStore, String trustStore, String password)
            throws IOException, GeneralSecurityException
    {
        SSLContext sslContext = SSLContextBuilder.create()
                .loadKeyMaterial(Paths.get(keyStore).toUri().toURL(), password.toCharArray(), password.toCharArray())
                .loadTrustMaterial(Paths.get(trustStore).toUri().toURL(), password.toCharArray())
                .build();

        return HttpClients.custom()
                .setConnectionManager(
                        PoolingHttpClientConnectionManagerBuilder.create()
                                .setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                                        .setSslContext(sslContext).setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                        .build()
                                ).build()
                )
                .build();
    }

    protected static class GlobalPropertyOverrideContextInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext>
    {
        @Rule
        public TemporaryFolder tempFolder = new TemporaryFolder();

        GlobalPropertyOverrideContextInitializer() throws IOException
        {
            tempFolder.create();
        }

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext)
        {
            addInlinedPropertiesToEnvironment(configurableApplicationContext,
                    "server.ssl.enabled=true",
                    "server.ssl.client-auth=need",
                    "server.ssl.refresh-rate-in-ms=" + REFRESH_RATE_IN_MS,
                    "metricsServer.enabled=true",
                    "metricsServer.port=0",
                    "metricsServer.ssl.enabled=true",
                    "metricsServer.ssl.client-auth=need",
                    "metricsServer.ssl.refresh-rate-in-ms=" + METRICS_REFRESH_RATE_IN_MS);
        }
    }
}