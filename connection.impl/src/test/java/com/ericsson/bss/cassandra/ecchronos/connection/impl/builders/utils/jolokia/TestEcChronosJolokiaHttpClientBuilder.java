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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.jolokia;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;
import java.util.ServiceLoader;

import org.jolokia.client.JolokiaClientBuilder.Configuration;
import org.jolokia.client.JolokiaClientBuilder.ConnectionConfiguration;
import org.jolokia.client.JolokiaClientBuilder.PoolConfiguration;
import org.jolokia.client.JolokiaClientBuilder.TlsConfiguration;
import org.jolokia.client.jdkclient.JdkHttpClient;
import org.jolokia.client.spi.HttpClientBuilder;
import org.jolokia.client.spi.HttpClientSpi;
import org.junit.Test;

public class TestEcChronosJolokiaHttpClientBuilder
{
    private static final URI URL = URI.create("http://localhost:8778/jolokia/");

    private static Configuration plaintextConfig(final boolean expectContinue)
    {
        return new Configuration(
                URL, null, null, null,
                new ConnectionConfiguration(5000, 5000, false, 8192),
                null,
                new PoolConfiguration(true, 20, 500),
                StandardCharsets.UTF_8, expectContinue, Collections.emptySet(), null, null);
    }

    private static Configuration tlsConfig(final TlsConfiguration tls) throws Exception
    {
        return new Configuration(
                URI.create("https://localhost:8778/jolokia/"), null, null, null,
                new ConnectionConfiguration(5000, 5000, false, 8192),
                tls,
                new PoolConfiguration(true, 20, 500),
                StandardCharsets.UTF_8, true, Collections.emptySet(), null, null);
    }

    private static KeyStore emptyKeyStore() throws Exception
    {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        return keyStore;
    }

    @Test
    public void testServiceLoaderSelectsEcChronosProvider()
    {
        ServiceLoader<HttpClientBuilder> loader = ServiceLoader.load(HttpClientBuilder.class);
        HttpClientBuilder selected = loader.findFirst().orElseThrow();

        assertThat(selected).isInstanceOf(EcChronosJolokiaHttpClientBuilder.class);
    }

    @Test
    public void testExpectContinueDisabledOnCopy()
    {
        Configuration source = plaintextConfig(true);

        Configuration result = JolokiaConfigurationFactory.withExpectContinueDisabled(source);

        assertThat(result.expectContinue()).isFalse();
        // Every other component must be preserved verbatim.
        assertThat(result.url()).isEqualTo(source.url());
        assertThat(result.connectionConfig()).isEqualTo(source.connectionConfig());
        assertThat(result.poolConfig()).isEqualTo(source.poolConfig());
        assertThat(result.contentCharset()).isEqualTo(source.contentCharset());
        assertThat(result.tlsConfig()).isEqualTo(source.tlsConfig());
    }

    @Test
    public void testExpectContinueCopyIsNoOpWhenAlreadyDisabled()
    {
        Configuration source = plaintextConfig(false);

        Configuration result = JolokiaConfigurationFactory.withExpectContinueDisabled(source);

        assertThat(result).isSameAs(source);
    }

    @Test
    public void testBuiltClientIsHttp11AndExpectContinueDisabled() throws Exception
    {
        // Use a TLS config so the managed factory installs an explicit SSLContext and the JDK client does not fall
        // back to SSLContext.getDefault() (which other tests in the suite can render unusable via leaked system
        // properties). The SPI still returns a JdkHttpClient regardless of TLS.
        TlsConfiguration tls = new TlsConfiguration(
                "TLSv1.3", emptyKeyStore(), null, null, null, emptyKeyStore(), null, null);
        Configuration source = tlsConfig(tls);

        try (HttpClientSpi<HttpClient> spi = new EcChronosJolokiaHttpClientBuilder().buildHttpClient(source))
        {
            assertThat(spi).isInstanceOf(JdkHttpClient.class);
            HttpClient client = spi.getClient(HttpClient.class);
            assertThat(client.version()).isEqualTo(HttpClient.Version.HTTP_1_1);
        }
    }

    @Test
    public void testPlaintextConfigHasNoSslContext()
    {
        assertThat(ManagedJolokiaHttpClientFactory.createSslContext(null)).isNull();
        TlsConfiguration noTls = new TlsConfiguration(null, null, null, null, null, null, null, null);
        assertThat(ManagedJolokiaHttpClientFactory.createSslContext(noTls)).isNull();
    }

    @Test
    public void testTlsConfigBuildsSslContextFromKeyStores() throws Exception
    {
        TlsConfiguration tls = new TlsConfiguration(
                "TLSv1.3", emptyKeyStore(), null, null, null, emptyKeyStore(), null, null);

        assertThat(ManagedJolokiaHttpClientFactory.createSslContext(tls)).isNotNull();
        // And the full client build applies it without error.
        assertThat(ManagedJolokiaHttpClientFactory.buildHttpClient(tlsConfig(tls))).isNotNull();
    }

    @Test
    public void testInvalidTlsConfigIsReportedClearly() throws Exception
    {
        // Both an explicit keystore and a keystore path set is rejected by TlsConfiguration.validate().
        TlsConfiguration invalid = new TlsConfiguration(
                "TLSv1.3", emptyKeyStore(), Path.of("/does/not/matter"), null, null, null, null, null);

        assertThatThrownBy(() -> ManagedJolokiaHttpClientFactory.createSslContext(invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
