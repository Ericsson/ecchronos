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

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKeyStoreFactory
{
    private static final String KEYSTORE_PASSWORD = "ecctest";
    private static final String STORE_TYPE_JKS = "JKS";
    private static final CertUtils CERT_UTILS = new CertUtils();

    private static String caCert;
    private static String clientCert;
    private static String clientKey;
    private static String keyStorePath;
    private static String trustStorePath;

    @BeforeClass
    public static void setup() throws IOException
    {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        String dir = folder.getRoot().getPath();

        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plus(Duration.ofHours(1)));

        caCert = Paths.get(dir, "ca.crt").toString();
        String caKey = Paths.get(dir, "ca.key").toString();
        CERT_UTILS.createSelfSignedCACertificate("testCA", notBefore, notAfter, "RSA", caCert, caKey);

        clientCert = Paths.get(dir, "client.crt").toString();
        clientKey = Paths.get(dir, "client.key").toString();
        CERT_UTILS.createCertificate("client", notBefore, notAfter, caCert, caKey, clientCert, clientKey);

        keyStorePath = Paths.get(dir, "client.keystore").toString();
        CERT_UTILS.createKeyStore(clientCert, clientKey, STORE_TYPE_JKS, KEYSTORE_PASSWORD, keyStorePath);

        trustStorePath = Paths.get(dir, "client.truststore").toString();
        CERT_UTILS.createTrustStore(caCert, STORE_TYPE_JKS, KEYSTORE_PASSWORD, trustStorePath);
    }

    @Test
    public void testCreatePemKeyManagerFactory() throws Exception
    {
        TLSConfig config = createPemConfig();
        KeyManagerFactory kmf = KeyStoreFactory.createPemKeyManagerFactory(config, STORE_TYPE_JKS);
        assertThat(kmf).isNotNull();
        assertThat(kmf.getKeyManagers()).isNotEmpty();
    }

    @Test
    public void testCreatePemTrustManagerFactory() throws Exception
    {
        TLSConfig config = createPemConfig();
        TrustManagerFactory tmf = KeyStoreFactory.createPemTrustManagerFactory(config, STORE_TYPE_JKS);
        assertThat(tmf).isNotNull();
        assertThat(tmf.getTrustManagers()).isNotEmpty();
    }

    @Test
    public void testCreateKeyManagerFactory() throws Exception
    {
        TLSConfig config = createJksConfig();
        KeyManagerFactory kmf = KeyStoreFactory.createKeyManagerFactory(config);
        assertThat(kmf).isNotNull();
        assertThat(kmf.getKeyManagers()).isNotEmpty();
    }

    @Test
    public void testCreateTrustManagerFactory() throws Exception
    {
        TLSConfig config = createJksConfig();
        TrustManagerFactory tmf = KeyStoreFactory.createTrustManagerFactory(config);
        assertThat(tmf).isNotNull();
        assertThat(tmf.getTrustManagers()).isNotEmpty();
    }

    @Test
    public void testCreateKeyManagerFactoryInvalidPathThrows()
    {
        CqlTLSConfig config = new CqlTLSConfig(true, "/nonexistent.keystore", KEYSTORE_PASSWORD,
                trustStorePath, KEYSTORE_PASSWORD);
        config.setStoreType(STORE_TYPE_JKS);

        assertThatThrownBy(() -> KeyStoreFactory.createKeyManagerFactory(config))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testCreateTrustManagerFactoryInvalidPathThrows()
    {
        CqlTLSConfig config = new CqlTLSConfig(true, keyStorePath, KEYSTORE_PASSWORD,
                "/nonexistent.truststore", KEYSTORE_PASSWORD);
        config.setStoreType(STORE_TYPE_JKS);

        assertThatThrownBy(() -> KeyStoreFactory.createTrustManagerFactory(config))
                .isInstanceOf(IOException.class);
    }

    private CqlTLSConfig createPemConfig()
    {
        CqlTLSConfig config = new CqlTLSConfig(true, clientCert, clientKey, caCert);
        config.setProtocol("TLSv1.2");
        return config;
    }

    private CqlTLSConfig createJksConfig()
    {
        CqlTLSConfig config = new CqlTLSConfig(true, keyStorePath, KEYSTORE_PASSWORD,
                trustStorePath, KEYSTORE_PASSWORD);
        config.setStoreType(STORE_TYPE_JKS);
        config.setProtocol("TLSv1.2");
        config.setCRLConfig(new CRLConfig());
        return config;
    }
}
