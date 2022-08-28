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

import static com.google.common.io.Resources.getResource;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestTomcatWebServerCustomizer
{
    private static final String CLIENT_VALID_PATH = "valid/";
    private static final String CLIENT_EXPIRED_PATH = "expired/";
    private static final int REFRESH_RATE = 100;
    private static final int INVOCATION_COUNT = 1;

    private String httpsUrl;

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
    private TomcatWebServerCustomizer tomcatWebServerCustomizer;

    @Autowired
    private Environment environment;

    @Before
    public void init()
    {
        String httpsPort = environment.getProperty("local.server.port");
        httpsUrl = "https://localhost:" + httpsPort + "/actuator/health";
    }

    @Test
    public void testSuccessfulCertificateReloading()
    {
        await().atMost(new Duration(REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                .untilAsserted(() -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadSslContext());
    }

    @Test
    public void testSuccessfulResponseWhenValidCertificate() throws IOException, GeneralSecurityException
    {
        HttpResponse response = configureHttpClient(CLIENT_VALID_PATH).execute(new HttpGet(httpsUrl));
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(HTTP_OK);
    }

    @Test
    public void testExceptionWhenExpiredCertificate() throws IOException, GeneralSecurityException
    {
        HttpClient httpClient = configureHttpClient(CLIENT_EXPIRED_PATH);
        assertThatExceptionOfType(SSLException.class)
                .isThrownBy(() -> httpClient.execute(new HttpGet(httpsUrl)));
    }

    private HttpClient configureHttpClient(String storePath) throws IOException, GeneralSecurityException
    {
        SSLContext sslContext = SSLContextBuilder.create()
                .loadKeyMaterial(getResource(storePath + "crt.p12"), "".toCharArray(), "".toCharArray())
                .loadTrustMaterial(new TrustAllStrategy())
                .build();

        return HttpClients.custom()
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();
    }

    static class PropertyOverrideContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext>
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
                    "server.ssl.refresh-rate-in-ms=" + REFRESH_RATE);
        }
    }
}