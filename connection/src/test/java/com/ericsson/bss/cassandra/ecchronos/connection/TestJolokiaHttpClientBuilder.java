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
package com.ericsson.bss.cassandra.ecchronos.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.time.Duration;

import javax.net.ssl.SSLContext;

import org.junit.Test;

public class TestJolokiaHttpClientBuilder
{
    @Test
    public void testPinsHttp11() throws Exception
    {
        HttpClient client = JolokiaHttpClientBuilder
                .newBuilder(SSLContext.getDefault(), Duration.ofSeconds(5)).build();

        assertThat(client.version()).isEqualTo(HttpClient.Version.HTTP_1_1);
    }

    @Test
    public void testInjectsProvidedSslContext() throws Exception
    {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);

        HttpClient client = JolokiaHttpClientBuilder
                .newBuilder(sslContext, Duration.ofSeconds(5)).build();

        assertThat(client.sslContext()).isSameAs(sslContext);
        assertThat(client.connectTimeout()).contains(Duration.ofSeconds(5));
    }

    @Test
    public void testNullSslContextUsesPlaintextDefault() throws Exception
    {
        // A null SSLContext must not be injected; the JDK default context is left in place (plaintext usage).
        HttpClient client = JolokiaHttpClientBuilder.newBuilder(null, null).build();

        assertThat(client.version()).isEqualTo(HttpClient.Version.HTTP_1_1);
        assertThat(client.sslContext()).isSameAs(SSLContext.getDefault());
        assertThat(client.connectTimeout()).isEmpty();
    }

    @Test
    public void testExpectContinueDisabled()
    {
        assertThat(JolokiaHttpClientBuilder.EXPECT_CONTINUE).isFalse();
    }
}
