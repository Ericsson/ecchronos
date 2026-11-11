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

import java.net.http.HttpClient;

import org.jolokia.client.JolokiaClientBuilder.Configuration;
import org.jolokia.client.jdkclient.JdkHttpClient;
import org.jolokia.client.spi.HttpClientBuilder;
import org.jolokia.client.spi.HttpClientSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Jolokia {@link HttpClientBuilder} that lets ecChronos own the Jolokia JMX adapter transport.
 *
 * <p>The Jolokia client resolves its transport via {@code ServiceLoader.load(HttpClientBuilder.class).findFirst()},
 * falling back to the built-in {@code JdkHttpClientBuilder} only when no provider is registered. Because the built-in
 * builder is <em>not</em> registered through {@code META-INF/services}, registering this class there makes the Jolokia
 * client select it for every {@code JolokiaClientBuilder} in the JVM. ecChronos is the only Jolokia user in the
 * process, so this process-wide override is intentional and acceptable. See issue #1970.
 *
 * <p>The implementation deliberately reuses the library's {@link JdkHttpClient} for request/response marshalling and
 * only takes ownership of the two things ecChronos needs to control:
 * <ul>
 *   <li>the {@link java.net.http.HttpClient} itself (pinned to HTTP/1.1, with a managed {@link javax.net.ssl.SSLContext}
 *       built from the supplied configuration) via {@link ManagedJolokiaHttpClientFactory};</li>
 *   <li>the {@code Expect: 100-continue} request flag, disabled by handing {@link JdkHttpClient} a configuration copy
 *       produced by {@link JolokiaConfigurationFactory}.</li>
 * </ul>
 */
public final class EcChronosJolokiaHttpClientBuilder implements HttpClientBuilder<HttpClient>
{
    private static final Logger LOG = LoggerFactory.getLogger(EcChronosJolokiaHttpClientBuilder.class);

    /**
     * Default constructor required by {@link java.util.ServiceLoader}.
     */
    public EcChronosJolokiaHttpClientBuilder()
    {
        // Instantiated by ServiceLoader
    }

    @Override
    public HttpClientSpi<HttpClient> buildHttpClient(final Configuration configuration)
    {
        LOG.debug("Building managed Jolokia HTTP client (HTTP/1.1, Expect:100-continue disabled)");
        HttpClient httpClient = ManagedJolokiaHttpClientFactory.buildHttpClient(configuration);
        Configuration withoutExpectContinue = JolokiaConfigurationFactory.withExpectContinueDisabled(configuration);
        return new JdkHttpClient(httpClient, withoutExpectContinue);
    }
}
