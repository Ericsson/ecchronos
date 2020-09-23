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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.net.ssl.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ExtendedRemoteEndpointAwareSslOptions;
import com.ericsson.bss.cassandra.ecchronos.application.config.TLSConfig;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

public class ReloadingCertificateHandler implements ExtendedRemoteEndpointAwareSslOptions
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadingCertificateHandler.class);

    private final AtomicReference<Context> currentContext = new AtomicReference<>();
    private final Supplier<TLSConfig> tlsConfigSupplier;

    public ReloadingCertificateHandler(Supplier<TLSConfig> tlsConfigSupplier)
    {
        this.tlsConfigSupplier = tlsConfigSupplier;
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel, EndPoint remoteEndpoint)
    {
        Context context = getContext();
        TLSConfig tlsConfig = context.getTlsConfig();
        SSLContext sslContext = context.getSSLContext();

        SSLEngine sslEngine;
        if (remoteEndpoint != null)
        {
            InetSocketAddress socketAddress = remoteEndpoint.resolve();
            sslEngine = sslContext.createSSLEngine(socketAddress.getHostName(), socketAddress.getPort());
        }
        else
        {
            sslEngine = sslContext.createSSLEngine();
        }
        sslEngine.setUseClientMode(true);

        if (tlsConfig.requiresEndpointVerification())
        {
            SSLParameters sslParameters = new SSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
        }

        tlsConfig.getCipherSuites().ifPresent(sslEngine::setEnabledCipherSuites);

        return new SslHandler(sslEngine);
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel, InetSocketAddress remoteEndpoint)
    {
        throw new UnsupportedOperationException("This method should never be used");
    }

    @Override
    public SslHandler newSSLHandler(SocketChannel channel)
    {
        throw new UnsupportedOperationException("This method should never be used");
    }

    private Context getContext()
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

    static final class Context
    {
        private final TLSConfig tlsConfig;
        private final SSLContext sslContext;

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

        SSLContext getSSLContext()
        {
            return sslContext;
        }
    }

    private static SSLContext createSSLContext(TLSConfig tlsConfig) throws IOException, NoSuchAlgorithmException,
            KeyStoreException, CertificateException, UnrecoverableKeyException, KeyManagementException
    {
        SSLContext sslContext = SSLContext.getInstance(tlsConfig.getProtocol());
        KeyManagerFactory keyManagerFactory = getKeyManagerFactory(tlsConfig);
        TrustManagerFactory trustManagerFactory = getTrustManagerFactory(tlsConfig);

        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext;
    }

    private static KeyManagerFactory getKeyManagerFactory(TLSConfig tlsConfig) throws IOException,
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

    private static TrustManagerFactory getTrustManagerFactory(TLSConfig tlsConfig)
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
