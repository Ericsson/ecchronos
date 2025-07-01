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
import com.ericsson.bss.cassandra.ecchronos.application.CustomCRLValidator;
import com.ericsson.bss.cassandra.ecchronos.application.CustomX509TrustManager;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import jakarta.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.KeyStoreException;
import java.security.KeyManagementException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ReloadingCertificateHandler implements CertificateHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadingCertificateHandler.class);

    private final AtomicReference<Context> currentContext = new AtomicReference<>();
    private final Supplier<CqlTLSConfig> myCqlTLSConfigSupplier;

    private static final String DEFAULT_STORE_TYPE_JKS = "JKS";

    public ReloadingCertificateHandler(final Supplier<CqlTLSConfig> cqlTLSConfigSupplier)
    {
        this.myCqlTLSConfigSupplier = cqlTLSConfigSupplier;
    }

    /**
     * Create new SSL Engine.
     *
     * @param remoteEndpoint
     *         the remote endpoint.
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

    protected static final class Context
    {
        private final CqlTLSConfig myTlsConfig;
        private final SslContext mySslContext;
        private final Map<String, String> myChecksums = new HashMap<>();

        Context(final CqlTLSConfig tlsConfig)
                throws NoSuchAlgorithmException, IOException, UnrecoverableKeyException, CertificateException,
                KeyStoreException, KeyManagementException, InvalidAlgorithmParameterException, CRLException
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
            return myTlsConfig.equals(newTLSConfig) && checksumSame(newTLSConfig);
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
        String storeType = tlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE_JKS);

        if (tlsConfig.isCertificateConfigured())
        {
             LOG.info("Will use PEM certificates for CQL connections, using internal store type {}", storeType);

            // Get certificate and key files from config
            File certificateFile = new File(tlsConfig.getCertificatePath().get());
            File certificatePrivateKeyFile = new File(tlsConfig.getCertificatePrivateKeyPath().get());
            File trustCertificateFile = new File(tlsConfig.getTrustCertificatePath().get());

            // Put client certificate(s) and its private key into an internal keystore
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(null, null);


            CertificateFactory cf = CertificateFactory.getInstance("X.509");

            // Read up all certificates in the file given (chained or not)
            try (FileInputStream fis = new FileInputStream(certificateFile))
            {
                List<Certificate> clientCertificates = new ArrayList<>(cf.generateCertificates(fis));
                if (clientCertificates.isEmpty())
                {
                    throw new CertificateException("No certificate(s) found in the certificate file!");
                }
                else
                {
                    // Order certs from leaf to root (the client cert will be at index 0 of the certificate array)
                    Certificate[] certChain = clientCertificates.toArray(new Certificate[0]);
                    LOG.debug("Number of certificates in certificate chain: {}", certChain.length);

                    // Load and parse the private key
                    PrivateKey privateKey = loadPrivateKey(certificatePrivateKeyFile);
                    LOG.debug("Private key algorithm: {}", privateKey.getAlgorithm());

                    // Finally, set the key/certs entry in the keystore
                    keyStore.setKeyEntry("client-certs", privateKey, "".toCharArray(), certChain);
                }
            }

            // Set trusted certificates
            // Create KeyManagerFactory
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, "".toCharArray());

            // Create TrustManagerFactory and load the trusted certificate into it
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore trustStore = KeyStore.getInstance(storeType);
            trustStore.load(null, null);

            try (FileInputStream fis = new FileInputStream(trustCertificateFile))
            {
                List<Certificate> trustedCertificates = new ArrayList<>(cf.generateCertificates(fis));
                if (trustedCertificates.isEmpty())
                {
                    throw new CertificateException("No certificate(s) found in the trusted certificate file!");
                }
                else
                {
                    LOG.debug("Number of certificates in trusted certificate chain: {}", trustedCertificates.size());
                    // Add all certificates in the trusted chain to the internal truststore
                    for (int i = 0; i < trustedCertificates.size(); i++)
                    {
                        trustStore.setCertificateEntry("trusted-certs" + i, trustedCertificates.get(i));
                    }

                    tmf.init(trustStore);
                }
            }

            // Finally, set the managers in the builder
            builder.keyManager(kmf);
            setTrustManagers(builder, tlsConfig, tmf);
        }
        else
        {
            LOG.info("Will use keystore/truststore for CQL connections, expecting store type {}", storeType);

            KeyManagerFactory keyManagerFactory = getKeyManagerFactory(tlsConfig);
            builder.keyManager(keyManagerFactory);
             setTrustManagers(builder, tlsConfig, getTrustManagerFactory(tlsConfig));
        }
        if (tlsConfig.getCipherSuites().isPresent())
        {
            builder.ciphers(Arrays.asList(tlsConfig.getCipherSuites().get()));
        }
        return builder.protocols(tlsConfig.getProtocols()).build();
    }
    protected static void setTrustManagers(final SslContextBuilder builder,
                                           final CqlTLSConfig config,
                                           final TrustManagerFactory tmf)
    {
        if (config.getCRLConfig().getEnabled())
        {
            // If CRL is enabled, use the CustomX509TrustManager (CRL checking is added to the SSL context)
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
            // No CRL, use TrustManagerFactory as-isAdd commentMore actions
            LOG.info("CRL not enabled");
            builder.trustManager(tmf);
        }

    }
    @SuppressWarnings("PMD.PreserveStackTrace")
    protected static PrivateKey loadPrivateKey(final File file) throws IOException
    {
        try
        {
            // Supported key types are EC and RSA (DSA is legacy and should not be used)

            // Read key file and remove the PEM header and any whitespaces
            String key = Files.readString(file.toPath());
            String privateKeyPEM = key
                    .replaceAll("-----BEGIN .*PRIVATE KEY-----", "")
                    .replaceAll("-----END .*PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");

            // Sanity check; no support for old style EC keys
            if (privateKeyPEM.contains("ECPARAMETERS"))
            {
                LOG.error("Old EC key format detected. Use new format instead!");
                throw new IOException("Unsupported private key EC format");
            }


            // Decode the key into a nice byte array
            byte[] decoded = Base64.getDecoder().decode(privateKeyPEM);
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decoded);

            try
            {
                LOG.debug("Trying private key type EC");
                KeyFactory kf = KeyFactory.getInstance("EC");
                return kf.generatePrivate(spec);
            }
            catch (InvalidKeySpecException e1)
            {
                LOG.debug("Trying private key type RSA");
                try
                {
                    KeyFactory kf = KeyFactory.getInstance("RSA");
                    return kf.generatePrivate(spec);
                }
                catch (InvalidKeySpecException e2)
                {
                    throw new IOException("Unsupported private key format", e2);
                }
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IOException("Failed to load the private key file", e);
        }

    }

    protected static KeyManagerFactory getKeyManagerFactory(final CqlTLSConfig tlsConfig) throws IOException,
            NoSuchAlgorithmException, KeyStoreException, CertificateException, UnrecoverableKeyException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(KeyManagerFactory.getDefaultAlgorithm());
        char[] keystorePassword = tlsConfig.getKeyStorePassword().toCharArray();

        try (InputStream keystoreFile = new FileInputStream(tlsConfig.getKeyStorePath()))
        {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE_JKS));
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
            KeyStore keyStore = KeyStore.getInstance(tlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE_JKS));
            keyStore.load(truststoreFile, truststorePassword);
            trustManagerFactory.init(keyStore);
            return trustManagerFactory;
        }
    }
}
