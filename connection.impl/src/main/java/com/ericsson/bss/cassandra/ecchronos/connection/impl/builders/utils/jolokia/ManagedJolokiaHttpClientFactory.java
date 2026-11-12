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

import java.net.CookieManager;
import java.net.http.HttpClient;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Duration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.jolokia.client.JolokiaClientBuilder.Configuration;
import org.jolokia.client.JolokiaClientBuilder.TlsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the {@link java.net.http.HttpClient} that ecChronos uses for the Jolokia JMX adapter transport.
 *
 * <p>The client is owned by ecChronos rather than by the Jolokia library, which gives us three guarantees independent
 * of the Jolokia version (see issue #1970):
 * <ul>
 *   <li>the HTTP protocol version is pinned to HTTP/1.1 explicitly (cf. #1964);</li>
 *   <li>the {@link SSLContext} is built here from the already-loaded key/trust stores on the supplied
 *       {@link Configuration#tlsConfig()}, so it is not governed solely by process-global {@code jolokia.*} system
 *       properties;</li>
 *   <li>a fresh client (and {@link SSLContext}) is produced on every connect. Because the Jolokia JMX adapter
 *       re-reads the certificate files from disk on each connect, a certificate rotation is picked up the next time the
 *       connector is rebuilt (reactively forced by the TLS-error invalidation added in #1960).</li>
 * </ul>
 *
 * <p>The client-building mirrors the Jolokia {@code JdkHttpClientBuilder} so that adapter behaviour is otherwise
 * unchanged.
 */
public final class ManagedJolokiaHttpClientFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(ManagedJolokiaHttpClientFactory.class);
    private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    private static final int CLIENT_PRIORITY = 1;
    private static final char[] EMPTY_PASSWORD = new char[0];

    private ManagedJolokiaHttpClientFactory()
    {
        // Utility class
    }

    /**
     * Build a managed HTTP/1.1 {@link java.net.http.HttpClient} for the supplied Jolokia configuration.
     *
     * @param configuration the Jolokia client configuration; must not be {@code null}.
     * @return a new HTTP/1.1 client, with the managed TLS context applied when the configuration describes a TLS
     *         connection and left unset (JDK default) for plaintext connections.
     */
    public static HttpClient buildHttpClient(final Configuration configuration)
    {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .cookieHandler(new CookieManager())
                .priority(CLIENT_PRIORITY);

        SSLContext sslContext = createSslContext(configuration.tlsConfig());
        if (sslContext != null)
        {
            builder.sslContext(sslContext);
        }

        if (configuration.connectionConfig() != null && configuration.connectionConfig().connectionTimeout() != -1)
        {
            builder.connectTimeout(Duration.ofMillis(configuration.connectionConfig().connectionTimeout()));
        }

        return builder.build();
    }

    /**
     * Build an {@link SSLContext} from the key/trust stores already loaded onto the Jolokia TLS configuration.
     *
     * <p>Returns {@code null} for plaintext connections, mirroring the Jolokia adapter which leaves
     * {@link TlsConfiguration#protocolVersion()} unset when TLS is not in use. The caller must not pass a {@code null}
     * context to {@link java.net.http.HttpClient.Builder#sslContext}.
     *
     * @param tlsConfiguration the Jolokia TLS configuration; may be {@code null}.
     * @return the managed TLS context, or {@code null} when no TLS is configured.
     */
    public static SSLContext createSslContext(final TlsConfiguration tlsConfiguration)
    {
        if (tlsConfiguration == null || tlsConfiguration.protocolVersion() == null)
        {
            return null;
        }
        try
        {
            tlsConfiguration.validate();
            SSLContext context = SSLContext.getInstance(tlsConfiguration.protocolVersion());
            context.init(
                    buildKeyManagers(tlsConfiguration),
                    buildTrustManagers(tlsConfiguration),
                    SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM));
            return context;
        }
        catch (GeneralSecurityException e)
        {
            throw new IllegalArgumentException("Problem with Jolokia TLS configuration: " + e.getMessage(), e);
        }
    }

    private static KeyManager[] buildKeyManagers(final TlsConfiguration tlsConfiguration)
            throws GeneralSecurityException
    {
        if (tlsConfiguration.keystore() == null)
        {
            return null;
        }
        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(tlsConfiguration.keystore(), keyPassword(tlsConfiguration));
        return keyManagerFactory.getKeyManagers();
    }

    private static TrustManager[] buildTrustManagers(final TlsConfiguration tlsConfiguration)
            throws GeneralSecurityException
    {
        if (tlsConfiguration.truststore() == null)
        {
            LOG.debug("No truststore configured for Jolokia; relying on the default trust material");
            return null;
        }
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tlsConfiguration.truststore());
        return trustManagerFactory.getTrustManagers();
    }

    private static char[] keyPassword(final TlsConfiguration tlsConfiguration)
    {
        return tlsConfiguration.keyPassword() == null
                ? EMPTY_PASSWORD
                : tlsConfiguration.keyPassword().toCharArray();
    }
}
