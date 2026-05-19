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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.List;

public final class KeyStoreFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(KeyStoreFactory.class);
    private static final String DEFAULT_STORE_TYPE = "JKS";

    private KeyStoreFactory()
    {
    }

    /**
     * Create a KeyManagerFactory from PEM certificate and private key files.
     */
    public static KeyManagerFactory createPemKeyManagerFactory(final TLSConfig tlsConfig, final String storeType)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException,
            UnrecoverableKeyException
    {
        File certificateFile = new File(tlsConfig.getCertificatePath().get());
        File keyFile = new File(tlsConfig.getCertificatePrivateKeyPath().get());

        List<Certificate> certs = CertificateLoader.loadCertificates(certificateFile);
        Certificate[] certChain = certs.toArray(new Certificate[0]);
        LOG.debug("Number of certificates in certificate chain: {}", certChain.length);

        PrivateKey privateKey = CertificateLoader.loadPrivateKey(keyFile);
        LOG.debug("Private key algorithm: {}", privateKey.getAlgorithm());

        KeyStore keyStore = KeyStore.getInstance(storeType);
        keyStore.load(null, null);
        keyStore.setKeyEntry("client-certs", privateKey, "".toCharArray(), certChain);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "".toCharArray());
        return kmf;
    }

    /**
     * Create a TrustManagerFactory from a PEM trust certificate file.
     */
    public static TrustManagerFactory createPemTrustManagerFactory(final TLSConfig tlsConfig, final String storeType)
            throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException
    {
        File trustCertFile = new File(tlsConfig.getTrustCertificatePath().get());
        List<Certificate> trustedCerts = CertificateLoader.loadCertificates(trustCertFile);
        LOG.debug("Number of certificates in trusted certificate chain: {}", trustedCerts.size());

        KeyStore trustStore = KeyStore.getInstance(storeType);
        trustStore.load(null, null);
        for (int i = 0; i < trustedCerts.size(); i++)
        {
            trustStore.setCertificateEntry("trusted-certs" + i, trustedCerts.get(i));
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf;
    }

    /**
     * Create a KeyManagerFactory from a keystore file (JKS/PKCS12).
     */
    public static KeyManagerFactory createKeyManagerFactory(final TLSConfig tlsConfig)
            throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException,
            UnrecoverableKeyException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(KeyManagerFactory.getDefaultAlgorithm());
        char[] password = tlsConfig.getKeyStorePassword().toCharArray();
        String storeType = tlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE);

        try (InputStream is = new FileInputStream(tlsConfig.getKeyStorePath()))
        {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(is, password);
            kmf.init(keyStore, password);
            return kmf;
        }
    }

    /**
     * Create a TrustManagerFactory from a truststore file (JKS/PKCS12).
     */
    public static TrustManagerFactory createTrustManagerFactory(final TLSConfig tlsConfig)
            throws IOException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        String algorithm = tlsConfig.getAlgorithm().orElse(TrustManagerFactory.getDefaultAlgorithm());
        char[] password = tlsConfig.getTrustStorePassword().toCharArray();
        String storeType = tlsConfig.getStoreType().orElse(DEFAULT_STORE_TYPE);

        try (InputStream is = new FileInputStream(tlsConfig.getTrustStorePath()))
        {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(is, password);
            tmf.init(keyStore);
            return tmf;
        }
    }
}
