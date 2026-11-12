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

import java.net.http.HttpClient;
import java.time.Duration;

import javax.net.ssl.SSLContext;

/**
 * Single definition of how ecChronos builds the {@link java.net.http.HttpClient} used for the Jolokia transport.
 *
 * <p>ecChronos talks to Jolokia over two channels - the notification channel ({@code JolokiaHttpClient} in core.impl)
 * and the JMX adapter channel ({@code ManagedJolokiaHttpClientFactory} in connection.impl, behind Jolokia's
 * {@code HttpClientBuilder} SPI). Both need the same cross-cutting HTTP settings, and historically maintained them
 * separately, which let them drift (HTTP/1.1 was pinned on each independently; {@code Expect: 100-continue} handling
 * differed). This helper centralizes the knobs that must agree so the same decision is made once. See issue #1972.
 *
 * <p>It intentionally returns a {@link HttpClient.Builder} rather than a built {@link HttpClient} so each channel can
 * add the extras it legitimately needs without those leaking across: the notification channel adds its own executor
 * and TLS session tuning; the adapter channel adds {@code followRedirects}/cookie/priority. Only the universally-safe
 * settings (HTTP version, connect timeout, TLS context) live here.
 *
 * <h2>SSLContext source asymmetry</h2>
 * The {@link SSLContext} is injected as a parameter because the two channels obtain TLS material differently and this
 * cannot be unified: the notification channel holds a reloadable {@link CertificateHandler}, whereas the adapter's SPI
 * builder is instantiated by {@link java.util.ServiceLoader} through a no-arg constructor and only receives a Jolokia
 * {@code Configuration} - there is no channel to pass a {@link CertificateHandler} in without out-of-band global
 * state. The adapter therefore derives its {@link SSLContext} from the connect-time configuration instead. See
 * issue #1972 (and #1970 for the adapter SPI).
 */
public final class JolokiaHttpClientBuilder
{
    /**
     * Whether Jolokia requests should send an {@code Expect: 100-continue} header. Kept {@code false} for both
     * channels: it is a root cause of intermittent {@code HTTP error 100} on the repair path (#1549 / #1473), and
     * Jolokia's simple request/response protocol gains nothing from it. Apply via
     * {@link java.net.http.HttpRequest.Builder#expectContinue(boolean)} where requests are built, or via the Jolokia
     * client configuration on the adapter path.
     */
    public static final boolean EXPECT_CONTINUE = false;

    private JolokiaHttpClientBuilder()
    {
        // Utility class
    }

    /**
     * Returns a {@link HttpClient.Builder} pre-configured with the settings that must be identical across the Jolokia
     * notification and adapter channels: HTTP/1.1, the supplied connect timeout, and the supplied TLS context.
     *
     * <p>HTTP/1.1 is pinned deliberately. The JDK HTTP client defaults to HTTP/2 and negotiates it via TLS ALPN;
     * TLS-terminating proxies in front of the Jolokia agent may terminate TLS without advertising ALPN, leaving the
     * client unable to agree on HTTP/2. Jolokia's HTTP API gains nothing from HTTP/2, so force HTTP/1.1 (cf. #1964,
     * #1970).
     *
     * @param sslContext    the TLS context to use, or {@code null} for a plaintext connection (the JDK default TLS
     *                     context is then left untouched, matching prior behaviour).
     * @param connectTimeout the connect timeout to apply, or {@code null} to leave the JDK default.
     * @return a builder the caller completes with any channel-specific extras before calling
     *         {@link HttpClient.Builder#build()}.
     */
    public static HttpClient.Builder newBuilder(final SSLContext sslContext, final Duration connectTimeout)
    {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1);
        if (sslContext != null)
        {
            builder.sslContext(sslContext);
        }
        if (connectTimeout != null)
        {
            builder.connectTimeout(connectTimeout);
        }
        return builder;
    }
}
