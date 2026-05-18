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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public final class CertificateLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(CertificateLoader.class);

    private CertificateLoader()
    {
    }

    /**
     * Load X.509 certificates from a PEM file.
     *
     * @param file the certificate file (may contain a chain).
     * @return list of certificates in file order.
     */
    public static List<Certificate> loadCertificates(final File file) throws IOException, CertificateException
    {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        try (FileInputStream fis = new FileInputStream(file))
        {
            List<Certificate> certs = new ArrayList<>(cf.generateCertificates(fis));
            if (certs.isEmpty())
            {
                throw new CertificateException("No certificate(s) found in file: " + file);
            }
            return certs;
        }
    }

    /**
     * Load a PKCS8-encoded private key from a PEM file. Supports EC and RSA key types.
     *
     * @param file the private key file.
     * @return the parsed private key.
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public static PrivateKey loadPrivateKey(final File file) throws IOException
    {
        try
        {
            String key = Files.readString(file.toPath());
            String privateKeyPEM = key
                    .replaceAll("-----BEGIN .*PRIVATE KEY-----", "")
                    .replaceAll("-----END .*PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");

            if (privateKeyPEM.contains("ECPARAMETERS"))
            {
                LOG.error("Old EC key format detected. Use new format instead!");
                throw new IOException("Unsupported private key EC format");
            }

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
}
