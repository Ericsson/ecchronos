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
import com.ericsson.bss.cassandra.ecchronos.application.config.security.CqlTLSConfig;
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
import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ReloadingCertificateHandler implements CertificateHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadingCertificateHandler.class);

    private final AtomicReference<Context> currentContext = new AtomicReference<>();
    private final Supplier<CqlTLSConfig> myCqlTLSConfigSupplier;

    public ReloadingCertificateHandler(final Supplier<CqlTLSConfig> cqlTLSConfigSupplier)
    {
        this.myCqlTLSConfigSupplier = cqlTLSConfigSupplier;
    }

    /**
     * Create new SSL Engine.
     *
     * @param remoteEndpoint the remote endpoint.
     * @return The SSLEngine.
     */
    @Override
    public SSLEngine newSslEngine(final EndPoint remoteEndpoint)
    {
        Context context = getContext();
        CqlTLSConfig tlsConfig = context.getTlsConfig();
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

    protected final Context getContext()
    {
        CqlTLSConfig tlsConfig = myCqlTLSConfigSupplier.get();
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
                tlsConfig = myCqlTLSConfigSupplier.get();
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
        private final CqlTLSConfig myTlsConfig;
        private final SslContext mySslContext;
        private final Map<String, String> myChecksums = new HashMap<>();

        Context(final CqlTLSConfig tlsConfig) throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
                CertificateException, KeyStoreException, KeyManagementException
        {
            myTlsConfig = tlsConfig;
            mySslContext = createSSLContext(myTlsConfig);
            myChecksums.putAll(calculateChecksums(myTlsConfig));
        }

        CqlTLSConfig getTlsConfig()
        {
            return myTlsConfig;
        }

        boolean sameConfig(final CqlTLSConfig newTLSConfig) throws IOException, NoSuchAlgorithmException
        {
            if (!myTlsConfig.equals(newTLSConfig))
            {
                return false;
            }
            return checksumSame(newTLSConfig);
        }

        private boolean checksumSame(final CqlTLSConfig newTLSConfig) throws IOException, NoSuchAlgorithmException
        {
            return myChecksums.equals(calculateChecksums(newTLSConfig));
        }

        private Map<String, String> calculateChecksums(final CqlTLSConfig tlsConfig)
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
            return DatatypeConverter.printHexBinary(digestBytes);
        }

        SslContext getSSLContext()
        {
            return mySslContext;
        }
    }

    protected static SslContext createSSLContext(final CqlTLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException,
            KeyStoreException,
            CertificateException,
            UnrecoverableKeyException
    {

        SslContextBuilder builder = SslContextBuilder.forClient();
        if (tlsConfig.isCertificateConfigured())
        {
            File certificateFile = new File(tlsConfig.getCertificatePath().get());
            File certificatePrivateKeyFile = new File(tlsConfig.getCertificatePrivateKeyPath().get());
            File trustCertificateFile = new File(tlsConfig.getTrustCertificatePath().get());

            builder.keyManager(certificateFile, certificatePrivateKeyFile);
            builder.trustManager(trustCertificateFile);
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

    protected static KeyManagerFactory getKeyManagerFactory(final CqlTLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException, KeyStoreException, CertificateException, UnrecoverableKeyException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(KeyManagerFactory.getDefaultAlgorithm());
        char[] keystorePassword = tlsConfig.getKeyStorePassword().toCharArray();

        try (InputStream keystoreFile = new FileInputStream(tlsConfig.getKeyStorePath()))
        {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType().orElse("JKS"));
            keyStore.load(keystoreFile, keystorePassword);
            keyManagerFactory.init(keyStore, keystorePassword);
            return keyManagerFactory;
        }
    }

    protected static TrustManagerFactory getTrustManagerFactory(final CqlTLSConfig tlsConfig)
            throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(TrustManagerFactory.getDefaultAlgorithm());
        char[] truststorePassword = tlsConfig.getTrustStorePassword().toCharArray();

        try (InputStream truststoreFile = new FileInputStream(tlsConfig.getTrustStorePath()))
        {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType().orElse("JKS"));
            keyStore.load(truststoreFile, truststorePassword);
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
    }
}
