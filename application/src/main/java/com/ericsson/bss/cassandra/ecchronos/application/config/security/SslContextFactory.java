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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import com.ericsson.bss.cassandra.ecchronos.application.CustomCRLValidator;
import com.ericsson.bss.cassandra.ecchronos.application.CustomX509TrustManager;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.util.Arrays;

/**
 * Factory class for creating Netty {@link SslContext} instances based on TLS configuration.
 * Supports custom CRL validation, endpoint verification, cipher suite selection, and protocol configuration.
 */
public final class SslContextFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(SslContextFactory.class);

    private SslContextFactory()
    {
    }

    /**
     * Build a Netty SslContext from the given managers and TLS configuration.
     *
     * @param tlsConfig the TLS configuration containing protocol, cipher, CRL, and endpoint verification settings.
     * @param kmf the key manager factory for client authentication.
     * @param tmf the trust manager factory for server certificate validation.
     * @return the configured SSL context.
     * @throws SSLException if the SSL context cannot be built.
     */
    public static SslContext create(final TLSConfig tlsConfig,
                                    final KeyManagerFactory kmf,
                                    final TrustManagerFactory tmf) throws SSLException
    {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.keyManager(kmf);
        setTrustManagers(builder, tlsConfig, tmf);

        if (tlsConfig.requiresEndpointVerification())
        {
            LOG.info("Endpoint verification enabled");
            builder.endpointIdentificationAlgorithm("HTTPS");
        }
        else
        {
            LOG.info("Endpoint verification disabled");
            builder.endpointIdentificationAlgorithm("");
        }

        if (tlsConfig.getCipherSuites().isPresent())
        {
            builder.ciphers(Arrays.asList(tlsConfig.getCipherSuites().get()));
        }

        return builder.protocols(tlsConfig.getProtocols()).build();
    }

    private static void setTrustManagers(final SslContextBuilder builder,
                                         final TLSConfig config,
                                         final TrustManagerFactory tmf)
    {
        if (config.getCRLConfig().getEnabled())
        {
            LOG.info("CRL enabled using strict mode: {}", config.getCRLConfig().getStrict());
            TrustManager[] trustManagers = tmf.getTrustManagers();
            for (int i = 0; i < trustManagers.length; i++)
            {
                if (trustManagers[i] instanceof X509TrustManager)
                {
                    CustomCRLValidator validator = new CustomCRLValidator(config.getCRLConfig());
                    trustManagers[i] = new CustomX509TrustManager(
                            (X509TrustManager) trustManagers[i],
                            validator
                    );
                    builder.trustManager(trustManagers[i]);
                }
            }
        }
        else
        {
            LOG.info("CRL not enabled");
            builder.trustManager(tmf);
        }
    }
}
