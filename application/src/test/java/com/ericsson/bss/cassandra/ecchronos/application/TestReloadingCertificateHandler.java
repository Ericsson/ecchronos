/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.application.config.TLSConfig;
import com.ericsson.bss.cassandra.ecchronos.application.utils.CertUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class TestReloadingCertificateHandler
{
    private static final String KEYSTORE_PASSWORD = "ecctest";
    private static final String STORE_TYPE_JKS = "JKS";

    private static CertUtils certUtils = new CertUtils();
    private static String certDirectory;
    private static String clientCaCert;
    private static String clientCaCertKey;
    private static String clientCert;
    private static String clientCertKey;
    private static String clientKeyStore;
    private static String clientTrustStore;

    //Using this we "modify" the config/keystore/certificate
    private String protocolVersion = "TLSv1.2";

    @BeforeClass
    public static void initOnce() throws IOException
    {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        certDirectory = temporaryFolder.getRoot().getPath();
        setupCerts();
    }

    private static void setupCerts()
    {
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plus(java.time.Duration.ofHours(1)));

        clientCaCert = Paths.get(certDirectory, "clientca.crt").toString();
        clientCaCertKey = Paths.get(certDirectory, "clientca.key").toString();
        certUtils.createSelfSignedCACertificate("clientCA", notBefore, notAfter, "RSA", clientCaCert, clientCaCertKey);
        clientCert = Paths.get(certDirectory, "client.crt").toString();
        clientCertKey = Paths.get(certDirectory, "client.key").toString();
        certUtils.createCertificate("client", notBefore, notAfter, clientCaCert, clientCaCertKey, clientCert, clientCertKey);
        clientKeyStore = Paths.get(certDirectory, "client.keystore").toString();
        certUtils.createKeyStore(clientCert, clientCertKey, STORE_TYPE_JKS, KEYSTORE_PASSWORD, clientKeyStore);
        clientTrustStore = Paths.get(certDirectory, "client.truststore").toString();
        certUtils.createTrustStore(clientCaCert, STORE_TYPE_JKS, KEYSTORE_PASSWORD, clientTrustStore);
    }

    @Test
    public void testNewSslEngineSameContextWhenConfigDoesNotChangeKeyStore()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithKeyStore());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isEqualTo(oldContext);
    }

    @Test
    public void testNewSslEngineDifferentContextWhenConfigChangesKeyStore()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithKeyStore());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        //Change protocolVersion to simulate a change in the config
        protocolVersion = "TLSv1.1,TLSv1.2";

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isNotEqualTo(oldContext);
    }

    @Test
    public void testNewSslEngineDifferentContextWhenKeyStoreChanges()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithKeyStore());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        //Create certificates again to simulate renewal
        setupCerts();

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isNotEqualTo(oldContext);
    }

    @Test
    public void testNewSslEngineSameContextWhenConfigDoesNotChangePEM()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithPEMFiles());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isEqualTo(oldContext);
    }

    @Test
    public void testNewSslEngineDifferentContextWhenConfigChangesPEM()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithPEMFiles());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        //Change protocolVersion to simulate a change in the config
        protocolVersion = "TLSv1.1,TLSv1.2";

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isNotEqualTo(oldContext);
    }

    @Test
    public void testNewSslEngineDifferentContextWhenCertificateChangesPEM()
    {
        ReloadingCertificateHandler reloadingCertificateHandler = new ReloadingCertificateHandler(() -> getTLSConfigWithPEMFiles());

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context oldContext = reloadingCertificateHandler.getContext();

        //Create certificates again to simulate renewal
        setupCerts();

        reloadingCertificateHandler.newSslEngine(null);
        ReloadingCertificateHandler.Context newContext = reloadingCertificateHandler.getContext();

        assertThat(newContext).isNotEqualTo(oldContext);
    }

    private TLSConfig getTLSConfigWithKeyStore()
    {
        TLSConfig tlsConfig = new TLSConfig();
        tlsConfig.setKeystore(clientKeyStore);
        tlsConfig.setTruststore(clientTrustStore);
        tlsConfig.setKeystore_password(KEYSTORE_PASSWORD);
        tlsConfig.setTruststore_password(KEYSTORE_PASSWORD);
        tlsConfig.setStore_type(STORE_TYPE_JKS);
        tlsConfig.setProtocol(protocolVersion);
        return tlsConfig;
    }

    private TLSConfig getTLSConfigWithPEMFiles()
    {
        TLSConfig tlsConfig = new TLSConfig();
        tlsConfig.setCertificate(clientCert);
        tlsConfig.setCertificate_private_key(clientCertKey);
        tlsConfig.setTrust_certificate(clientCaCert);
        tlsConfig.setProtocol(protocolVersion);
        return tlsConfig;
    }
}
