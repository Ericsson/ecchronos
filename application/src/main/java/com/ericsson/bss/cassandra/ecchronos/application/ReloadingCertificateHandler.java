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
    private final Supplier<TLSConfig> tlsConfigSupplier;

    public ReloadingCertificateHandler(final Supplier<TLSConfig> aTLSConfigSupplier)
    {
        this.tlsConfigSupplier = aTLSConfigSupplier;
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

        if (tlsConfig.requiresEndpointVerification())
        {
            SSLParameters sslParameters = new SSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
        }
        tlsConfig.getCipher_suites().ifPresent(sslEngine::setEnabledCipherSuites);
        return sslEngine;
    }

    protected final Context getContext()
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
        private final TLSConfig myTlsConfig;
        private final SslContext mySslContext;
        private final Map<String, String> myChecksums = new HashMap<>();

        Context(final TLSConfig tlsConfig) throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException,
                CertificateException, KeyStoreException, KeyManagementException
        {
            myTlsConfig = tlsConfig;
            mySslContext = createSSLContext(myTlsConfig);
            myChecksums.putAll(calculateChecksums(myTlsConfig));
        }

        TLSConfig getTlsConfig()
        {
            return myTlsConfig;
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
            if (tlsConfig.getCertificate().isPresent()
                    && tlsConfig.getCertificatePrivateKey().isPresent()
                    && tlsConfig.getTrustCertificate().isPresent())
            {
                String certificate = tlsConfig.getCertificate().get();
                checksums.put(certificate, getChecksum(certificate));
                String certificatePrivateKey = tlsConfig.getCertificatePrivateKey().get();
                checksums.put(certificatePrivateKey, getChecksum(certificatePrivateKey));
                String trustCertificate = tlsConfig.getTrustCertificate().get();
                checksums.put(trustCertificate, getChecksum(trustCertificate));
            }
            else
            {
                String keyStore = tlsConfig.getKeystore();
                checksums.put(keyStore, getChecksum(keyStore));
                String trustStore = tlsConfig.getTruststore();
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

    protected static SslContext createSSLContext(final TLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException,
            KeyStoreException,
            CertificateException,
            UnrecoverableKeyException
    {

        SslContextBuilder builder = SslContextBuilder.forClient();

        if (tlsConfig.getCertificate().isPresent()
                && tlsConfig.getCertificatePrivateKey().isPresent()
                && tlsConfig.getTrustCertificate().isPresent())
        {
            File certificateFile = new File(tlsConfig.getCertificate().get());
            File certificatePrivateKeyFile = new File(tlsConfig.getCertificatePrivateKey().get());
            File trustCertificateFile = new File(tlsConfig.getTrustCertificate().get());

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
        if (tlsConfig.getCipher_suites().isPresent())
        {
            builder.ciphers(Arrays.asList(tlsConfig.getCipher_suites().get()));
        }
        return builder.protocols(tlsConfig.getProtocols()).build();
    }

    protected static KeyManagerFactory getKeyManagerFactory(final TLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException, KeyStoreException, CertificateException, UnrecoverableKeyException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(KeyManagerFactory.getDefaultAlgorithm());
        char[] keystorePassword = tlsConfig.getKeystore_password().toCharArray();

        try (InputStream keystoreFile = new FileInputStream(tlsConfig.getKeystore()))
        {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStore_type());
            keyStore.load(keystoreFile, keystorePassword);
            keyManagerFactory.init(keyStore, keystorePassword);
            return keyManagerFactory;
        }
    }

    protected static TrustManagerFactory getTrustManagerFactory(final TLSConfig tlsConfig)
            throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(TrustManagerFactory.getDefaultAlgorithm());
        char[] truststorePassword = tlsConfig.getTruststore_password().toCharArray();

        try (InputStream truststoreFile = new FileInputStream(tlsConfig.getTruststore()))
        {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStore_type());
            keyStore.load(truststoreFile, truststorePassword);
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
    }
}
