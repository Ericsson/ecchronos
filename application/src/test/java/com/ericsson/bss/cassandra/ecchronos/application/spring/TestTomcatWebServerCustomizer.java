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

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Resources.getResource;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

public abstract class TestTomcatWebServerCustomizer
{
    private static final String DEFAULT_CLIENT_VALID_PATH = "valid-rsa/";
    private static final String DEFAULT_CLIENT_EXPIRED_PATH = "expired-rsa/";
    private static final String DEFAULT_CLIENT_PASSWORD = "";
    private static final String DEFAULT_METRICS_CLIENT_VALID_PATH = "metrics-valid-rsa/";
    private static final String DEFAULT_METRICS_CLIENT_EXPIRED_PATH = "metrics-expired-rsa/";
    private static final String DEFAULT_METRICS_CLIENT_PASSWORD = "ecctest";

    protected static final int REFRESH_RATE = 100;
    protected static final int METRICS_REFRESH_RATE = 50;
    protected static final int INVOCATION_COUNT = 1;

    protected String clientValidPath;
    protected String clientExpiredPath;
    protected String clientPassword;
    protected String metricsClientValidPath;
    protected String metricsClientExpiredPath;
    protected String metricsClientPassword;

    private String httpsUrl;
    private String metricsServerHttpsUrl;

    @Autowired
    private Environment environment;

    @MockBean
    private ECChronos ecChronos;

    @MockBean
    private NativeConnectionProvider nativeConnectionProvider;

    @MockBean
    private JmxConnectionProvider jmxConnectionProvider;

    @MockBean
    private ReplicationState replicationState;

    @MockBean
    private NodeResolver nodeResolver;

    @MockBean
    private RepairHistoryBean repairHistoryBean;

    @MockBean
    private CassandraHealthIndicator cassandraHealthIndicator;

    @SpyBean
    protected TomcatWebServerCustomizer tomcatWebServerCustomizer;

    @Before
    public void init()
    {
        String httpsPort = environment.getProperty("local.server.port");
        httpsUrl = "https://localhost:" + httpsPort + "/actuator/health";

        tomcatWebServerCustomizer.setMetricsPortProperty();
        String metricsHttpsPort = environment.getProperty("metricsServer.local.port");
        metricsServerHttpsUrl = "https://localhost:" + metricsHttpsPort + "/actuator/health";

        clientValidPath = DEFAULT_CLIENT_VALID_PATH;
        clientExpiredPath = DEFAULT_CLIENT_EXPIRED_PATH;
        clientPassword = DEFAULT_CLIENT_PASSWORD;
        metricsClientValidPath = DEFAULT_METRICS_CLIENT_VALID_PATH;
        metricsClientExpiredPath = DEFAULT_METRICS_CLIENT_EXPIRED_PATH;
        metricsClientPassword = DEFAULT_METRICS_CLIENT_PASSWORD;
    }

    @Test
    public void testSuccessfulCertificateReloading()
    {
        await().atMost(new Duration(REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                .untilAsserted(() -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadSslContext());
        await().atMost(new Duration(METRICS_REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                .untilAsserted(() -> verify(tomcatWebServerCustomizer,
                        atLeast(INVOCATION_COUNT)).reloadMetricsServerSslContext());
    }

    @Test
    public void testSuccessfulResponseWhenValidCertificate() throws IOException, GeneralSecurityException
    {
        HttpResponse response = configureHttpClient(clientValidPath, clientPassword).execute(new HttpGet(httpsUrl));
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(HTTP_OK);
        HttpResponse metricsResponse = configureHttpClient(metricsClientValidPath,
                metricsClientPassword).execute(new HttpGet(metricsServerHttpsUrl));
        assertThat(metricsResponse.getStatusLine().getStatusCode()).isEqualTo(HTTP_OK);
    }

    @Test
    public void testExceptionWhenExpiredCertificate() throws IOException, GeneralSecurityException
    {
        HttpClient httpClient = configureHttpClient(clientExpiredPath, clientPassword);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> httpClient.execute(new HttpGet(httpsUrl)));
        HttpClient metricsHttpClient = configureHttpClient(metricsClientExpiredPath, metricsClientPassword);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> metricsHttpClient.execute(new HttpGet(metricsServerHttpsUrl)));
    }

    @Test
    public void testExceptionWhenCertificateForWrongServer() throws IOException, GeneralSecurityException
    {
        HttpClient httpClient = configureHttpClient(clientValidPath, clientPassword);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> httpClient.execute(new HttpGet(metricsServerHttpsUrl)));
        HttpClient metricsHttpClient = configureHttpClient(metricsClientValidPath,
                metricsClientPassword);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> metricsHttpClient.execute(new HttpGet(httpsUrl)));
    }

    private HttpClient configureHttpClient(String storePath, String password)
            throws IOException, GeneralSecurityException
    {
        SSLContext sslContext = SSLContextBuilder.create()
                .loadKeyMaterial(getResource(storePath + "crt.p12"), password.toCharArray(), password.toCharArray())
                .loadTrustMaterial(getResource(storePath + "trust.p12"), password.toCharArray())
                .build();

        return HttpClients.custom()
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();
    }

    static class PropertyOverrideContextInitializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext>
    {
        @Rule
        public TemporaryFolder tempFolder = new TemporaryFolder();

        PropertyOverrideContextInitializer() throws IOException
        {
            tempFolder.create();
        }

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext)
        {
            addInlinedPropertiesToEnvironment(configurableApplicationContext,
                    "server.ssl.enabled=true",
                    "server.ssl.client-auth=need",
                    "server.ssl.refresh-rate-in-ms=" + REFRESH_RATE,
                    "metricsServer.enabled=true",
                    "metricsServer.port=0",
                    "metricsServer.ssl.enabled=true",
                    "metricsServer.ssl.client-auth=need",
                    "metricsServer.ssl.refresh-rate-in-ms=" + METRICS_REFRESH_RATE);
        }
    }
}