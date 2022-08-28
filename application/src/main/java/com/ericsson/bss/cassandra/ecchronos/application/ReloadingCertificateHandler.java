/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.ericsson.bss.cassandra.ecchronos.application.config.TLSConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ReloadingCertificateHandler implements CertificateHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadingCertificateHandler.class);

    private final AtomicReference<Context> currentContext = new AtomicReference<>();
    private final Supplier<TLSConfig> tlsConfigSupplier;

    public ReloadingCertificateHandler(Supplier<TLSConfig> tlsConfigSupplier)
    {
        this.tlsConfigSupplier = tlsConfigSupplier;
    }

    @Override
    public SSLEngine newSslEngine(EndPoint remoteEndpoint)
    {
        Context context = getContext();
        TLSConfig tlsConfig = context.getTlsConfig();
        SslContext sslContext = context.getSSLContext();

        SSLEngine sslEngine;
        if (remoteEndpoint != null)
        {
            InetSocketAddress socketAddress = (InetSocketAddress) remoteEndpoint.resolve();
            sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT, socketAddress.getHostName(),
                    socketAddress.getPort());
        }
        else
        {
            sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT);
        }
        sslEngine.setUseClientMode(true);

        if (tlsConfig.requiresEndpointVerification())
        {
            SSLParameters sslParameters = new SSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
        }
        tlsConfig.getCipherSuites().ifPresent(sslEngine::setEnabledCipherSuites);
        return sslEngine;
    }

    protected Context getContext()
    {
        TLSConfig tlsConfig = tlsConfigSupplier.get();
        Context context = currentContext.get();

        try
        {
            while (context == null || !context.sameConfig(tlsConfig))
            {
                Context newContext = new Context(tlsConfig);
                if (currentContext.compareAndSet(context, newContext))
                {
                    context = newContext;
                }
                else
                {
                    context = currentContext.get();
                }
                tlsConfig = tlsConfigSupplier.get();
            }
        }
        catch (NoSuchAlgorithmException | IOException | UnrecoverableKeyException | CertificateException
                | KeyStoreException | KeyManagementException e)
        {
            LOG.warn("Unable to create new SSL Context after configuration changed. Trying with the old one", e);
        }

        return context;
    }

    @Override
    public void close() throws Exception
    {

    }

    protected static final class Context
    {
        private final TLSConfig tlsConfig;
        private final SslContext sslContext;

        Context(TLSConfig tlsConfig) throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
                CertificateException, KeyStoreException, KeyManagementException
        {
            this.tlsConfig = tlsConfig;
            this.sslContext = createSSLContext(this.tlsConfig);
        }

        TLSConfig getTlsConfig()
        {
            return tlsConfig;
        }

        boolean sameConfig(TLSConfig tlsConfig)
        {
            return this.tlsConfig.equals(tlsConfig);
        }

        SslContext getSSLContext()
        {
            return sslContext;
        }
    }

    protected static SslContext createSSLContext(TLSConfig tlsConfig) throws IOException, NoSuchAlgorithmException,
            KeyStoreException, CertificateException, UnrecoverableKeyException
    {

        SslContextBuilder builder = SslContextBuilder.forClient();

        if (tlsConfig.getCertificate().isPresent() &&
                tlsConfig.getCertificateKey().isPresent() &&
                tlsConfig.getCertificateAuthorities().isPresent())
        {
            File certificateFile = new File(tlsConfig.getCertificate().get());
            File certificateKeyFile = new File(tlsConfig.getCertificateKey().get());
            File certificateAuthorityFile = new File(tlsConfig.getCertificateAuthorities().get());

            builder.keyManager(certificateFile, certificateKeyFile);
            builder.trustManager(certificateAuthorityFile);
        }
        else
        {
            KeyManagerFactory keyManagerFactory = getKeyManagerFactory(tlsConfig);
            TrustManagerFactory trustManagerFactory = getTrustManagerFactory(tlsConfig);
            builder.keyManager(keyManagerFactory);
            builder.trustManager(trustManagerFactory);
        }
        if (tlsConfig.getCipherSuites().isPresent())
        {
            builder.ciphers(Arrays.asList(tlsConfig.getCipherSuites().get()));
        }
        return builder.protocols(tlsConfig.getProtocols()).build();
    }

    protected static KeyManagerFactory getKeyManagerFactory(TLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException, KeyStoreException, CertificateException, UnrecoverableKeyException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(KeyManagerFactory.getDefaultAlgorithm());
        char[] keystorePassword = tlsConfig.getKeystorePassword().toCharArray();

        try (InputStream keystoreFile = new FileInputStream(tlsConfig.getKeystore()))
        {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType());
            keyStore.load(keystoreFile, keystorePassword);
            keyManagerFactory.init(keyStore, keystorePassword);
            return keyManagerFactory;
        }
    }

    protected static TrustManagerFactory getTrustManagerFactory(TLSConfig tlsConfig)
            throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(TrustManagerFactory.getDefaultAlgorithm());
        char[] truststorePassword = tlsConfig.getTruststorePassword().toCharArray();

        try (InputStream truststoreFile = new FileInputStream(tlsConfig.getTruststore()))
        {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType());
            keyStore.load(truststoreFile, truststorePassword);
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
    }
}
