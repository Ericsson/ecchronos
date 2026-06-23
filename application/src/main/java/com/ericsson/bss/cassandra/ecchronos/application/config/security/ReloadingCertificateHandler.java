/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.util.HexFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.KeyManagementException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.InvalidAlgorithmParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ReloadingCertificateHandler implements CertificateHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadingCertificateHandler.class);
    private static final String DEFAULT_STORE_TYPE_JKS = "JKS";

    private final AtomicReference<Context> currentContext = new AtomicReference<>();
    private final Supplier<TLSConfig> myTLSConfigSupplier;

    public ReloadingCertificateHandler(final Supplier<TLSConfig> tlsConfigSupplier)
    {
        this.myTLSConfigSupplier = tlsConfigSupplier;
    }

    @Override
    public final SSLEngine newSslEngine(final EndPoint remoteEndpoint)
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
        tlsConfig.getCipherSuites().ifPresent(sslEngine::setEnabledCipherSuites);
        return sslEngine;
    }

    protected final Context getContext()
    {
        TLSConfig tlsConfig = myTLSConfigSupplier.get();
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
                tlsConfig = myTLSConfigSupplier.get();
            }
        }
        catch (NoSuchAlgorithmException | IOException | UnrecoverableKeyException | CertificateException
               | KeyStoreException | KeyManagementException | InvalidAlgorithmParameterException | CRLException e)
        {
            LOG.warn("Unable to create new SSL Context after configuration changed. Trying with the old one", e);
        }

        return context;
    }

    @Override
    public void close() throws Exception
    {
    }

    private static final int SSL_SESSION_CACHE_SIZE = 50;
    private static final int SSL_SESSION_TIMEOUT_SECONDS = 3600;

    private volatile SSLContext myCachedSSLContext;
    private volatile Context myCachedSSLContextSource;

    @Override
    public final void setDefaultSSLContext()
    {
        Context context = getContext();
        if (context != null && context.getTlsConfig().isCertificateConfigured())
        {
            try
            {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                initSSLContext(context, sslContext);
                sslContext.getClientSessionContext().setSessionCacheSize(SSL_SESSION_CACHE_SIZE);
                sslContext.getClientSessionContext().setSessionTimeout(SSL_SESSION_TIMEOUT_SECONDS);
                SSLContext.setDefault(sslContext);
                LOG.info("Default SSLContext set for JVM");
            }
            catch (Exception e)
            {
                LOG.warn("Failed to set default SSLContext", e);
            }
        }
    }

    @Override
    public final SSLContext getSSLContext()
    {
        Context context = getContext();
        if (context == null || !context.getTlsConfig().isCertificateConfigured())
        {
            return null;
        }
        if (myCachedSSLContext != null && myCachedSSLContextSource == context) // NOPMD CompareObjectsWithEquals - intentional identity check
        {
            return myCachedSSLContext;
        }
        try
        {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            initSSLContext(context, sslContext);
            sslContext.getClientSessionContext().setSessionCacheSize(SSL_SESSION_CACHE_SIZE);
            sslContext.getClientSessionContext().setSessionTimeout(SSL_SESSION_TIMEOUT_SECONDS);
            myCachedSSLContext = sslContext;
            myCachedSSLContextSource = context;
            return sslContext;
        }
        catch (Exception e)
        {
            LOG.warn("Failed to create SSLContext", e);
            return null;
        }
    }

    private void initSSLContext(final Context context, final SSLContext sslContext) throws KeyManagementException
    {
        TrustManager[] trustManagers = context.getTrustManagerFactory() != null
                ? context.getTlsConfig().requiresEndpointVerification()
                    ? context.getTrustManagerFactory().getTrustManagers()
                    : buildNoEndpointValidationTrustManagers(context)
                : null;
        sslContext.init(
            context.getKeyManagerFactory() != null ? context.getKeyManagerFactory().getKeyManagers() : null,
            trustManagers,
            null
        );
    }

    private TrustManager[] buildNoEndpointValidationTrustManagers(final Context context)
    {
        TrustManager[] originalTrustManagers = context.getTrustManagerFactory().getTrustManagers();
        TrustManager[] trustManagers = new TrustManager[originalTrustManagers.length];
        for (int i = 0; i < originalTrustManagers.length; i++)
        {
            if (originalTrustManagers[i] instanceof X509ExtendedTrustManager)
            {
                trustManagers[i] = new NoEndpointValidationTrustManager(
                    (X509ExtendedTrustManager) originalTrustManagers[i]);
            }
            else
            {
                trustManagers[i] = originalTrustManagers[i];
            }
        }
        return trustManagers;
    }

    protected static final class Context
    {
        private final TLSConfig myTlsConfig;
        private final SslContext mySslContext;
        private final Map<String, String> myChecksums = new HashMap<>();
        private KeyManagerFactory myKeyManagerFactory;
        private TrustManagerFactory myTrustManagerFactory;

        Context(final TLSConfig tlsConfig)
                throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException, CertificateException,
                KeyStoreException, KeyManagementException, InvalidAlgorithmParameterException, CRLException
        {
            myTlsConfig = tlsConfig;
            buildManagers();
            mySslContext = SslContextFactory.create(myTlsConfig, myKeyManagerFactory, myTrustManagerFactory);
            myChecksums.putAll(calculateChecksums(myTlsConfig));
        }

        private void buildManagers() throws IOException, CertificateException, KeyStoreException,
                NoSuchAlgorithmException, UnrecoverableKeyException
        {
            String storeType = myTlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE_JKS);
            if (myTlsConfig.isCertificateConfigured())
            {
                LOG.info("Will use PEM certificates for connections, using internal store type {}", storeType);
                myKeyManagerFactory = KeyStoreFactory.createPemKeyManagerFactory(myTlsConfig, storeType);
                myTrustManagerFactory = KeyStoreFactory.createPemTrustManagerFactory(myTlsConfig, storeType);
            }
            else
            {
                LOG.info("Will use keystore/truststore for connections, expecting store type {}", storeType);
                myKeyManagerFactory = KeyStoreFactory.createKeyManagerFactory(myTlsConfig);
                myTrustManagerFactory = KeyStoreFactory.createTrustManagerFactory(myTlsConfig);
            }
        }

        KeyManagerFactory getKeyManagerFactory()
        {
            return myKeyManagerFactory;
        }

        TrustManagerFactory getTrustManagerFactory()
        {
            return myTrustManagerFactory;
        }

        TLSConfig getTlsConfig()
        {
            return myTlsConfig;
        }

        SslContext getSSLContext()
        {
            return mySslContext;
        }

        boolean sameConfig(final TLSConfig newTLSConfig) throws IOException, NoSuchAlgorithmException
        {
            return myTlsConfig.equals(newTLSConfig) && checksumSame(newTLSConfig);
        }

        private boolean checksumSame(final TLSConfig newTLSConfig) throws IOException, NoSuchAlgorithmException
        {
            return myChecksums.equals(calculateChecksums(newTLSConfig));
        }

        private Map<String, String> calculateChecksums(final TLSConfig tlsConfig)
                throws IOException, NoSuchAlgorithmException
        {
            Map<String, String> checksums = new HashMap<>();
            if (tlsConfig.isCertificateConfigured())
            {
                String certificate = tlsConfig.getCertificatePath().get();
                checksums.put(certificate, getChecksum(certificate));
                String certificatePrivateKey = tlsConfig.getCertificatePrivateKeyPath().get();
                checksums.put(certificatePrivateKey, getChecksum(certificatePrivateKey));
                String trustCertificate = tlsConfig.getTrustCertificatePath().get();
                checksums.put(trustCertificate, getChecksum(trustCertificate));
            }
            else
            {
                String keyStore = tlsConfig.getKeyStorePath();
                checksums.put(keyStore, getChecksum(keyStore));
                String trustStore = tlsConfig.getTrustStorePath();
                checksums.put(trustStore, getChecksum(trustStore));
            }
            return checksums;
        }

        private String getChecksum(final String file) throws IOException, NoSuchAlgorithmException
        {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digestBytes = md5.digest(Files.readAllBytes(Paths.get(file)));
            return HexFormat.of().formatHex(digestBytes);
        }
    }
}
