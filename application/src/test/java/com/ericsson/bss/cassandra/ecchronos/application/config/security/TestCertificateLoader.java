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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCertificateLoader
{
    private static final CertUtils CERT_UTILS = new CertUtils();
    private static String rsaCert;
    private static String rsaKey;
    private static String ecCert;
    private static String ecKey;

    @BeforeClass
    public static void setup() throws IOException
    {
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        String dir = folder.getRoot().getPath();

        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plus(Duration.ofHours(1)));

        rsaCert = Paths.get(dir, "rsa-ca.crt").toString();
        rsaKey = Paths.get(dir, "rsa-ca.key").toString();
        CERT_UTILS.createSelfSignedCACertificate("rsaCA", notBefore, notAfter, "RSA", rsaCert, rsaKey);

        ecCert = Paths.get(dir, "ec-ca.crt").toString();
        ecKey = Paths.get(dir, "ec-ca.key").toString();
        CERT_UTILS.createSelfSignedCACertificate("ecCA", notBefore, notAfter, "EC", ecCert, ecKey);
    }

    @Test
    public void testLoadCertificatesRSA() throws Exception
    {
        List<Certificate> certs = CertificateLoader.loadCertificates(new File(rsaCert));
        assertThat(certs).hasSize(1);
        assertThat(certs.get(0).getType()).isEqualTo("X.509");
    }

    @Test
    public void testLoadCertificatesEC() throws Exception
    {
        List<Certificate> certs = CertificateLoader.loadCertificates(new File(ecCert));
        assertThat(certs).hasSize(1);
    }

    @Test
    public void testLoadCertificatesEmptyFileThrows() throws Exception
    {
        File empty = File.createTempFile("empty", ".crt");
        empty.deleteOnExit();

        assertThatThrownBy(() -> CertificateLoader.loadCertificates(empty))
                .isInstanceOf(CertificateException.class)
                .hasMessageContaining("No certificate(s) found");
    }

    @Test
    public void testLoadCertificatesNonExistentFileThrows()
    {
        assertThatThrownBy(() -> CertificateLoader.loadCertificates(new File("/nonexistent.crt")))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testLoadPrivateKeyRSA() throws Exception
    {
        PrivateKey key = CertificateLoader.loadPrivateKey(new File(rsaKey));
        assertThat(key.getAlgorithm()).isEqualTo("RSA");
    }

    @Test
    public void testLoadPrivateKeyEC() throws Exception
    {
        PrivateKey key = CertificateLoader.loadPrivateKey(new File(ecKey));
        assertThat(key.getAlgorithm()).isEqualTo("EC");
    }

    @Test
    public void testLoadPrivateKeyInvalidFormatThrows() throws Exception
    {
        File invalid = File.createTempFile("invalid", ".key");
        invalid.deleteOnExit();
        Files.writeString(invalid.toPath(), "-----BEGIN PRIVATE KEY-----\nnotvalidbase64!\n-----END PRIVATE KEY-----\n");

        assertThatThrownBy(() -> CertificateLoader.loadPrivateKey(invalid))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void testLoadPrivateKeyNonExistentFileThrows()
    {
        assertThatThrownBy(() -> CertificateLoader.loadPrivateKey(new File("/nonexistent.key")))
                .isInstanceOf(IOException.class);
    }
}
