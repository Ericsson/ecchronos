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

import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509ExtensionUtils;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;

public class CertUtils
{
    public static final String RSA_ALGORITHM_NAME = "RSA";
    public static final String EC_ALGORITHM_NAME = "ECDSA";
    // When changing key sizes, make sure you know what you are doing.
    // Too big key sizes will slow down key pair generation by A LOT.
    // 2048 for RSA is not secure enough in the real world, but since this is only for tests it's perfectly fine.
    private static final int RSA_KEY_SIZE = 2048;
    private static final int EC_KEY_SIZE = 384;
    private static final int PEM_ENCODED_LINE_LENGTH = 64;
    private static final String DEFAULT_CA_ALIAS = "cacert";
    private static final String DEFAULT_CERT_ALIAS = "cert";
    private static final String RSA_HASH_ALGORITHM = "SHA256WithRSA";
    private static final String EC_HASH_ALGORITHM = "SHA256withECDSA";
    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----\n";
    private static final String END_CERTIFICATE = "\n-----END CERTIFICATE-----\n";
    private static final String BEGIN_PRIVATE_KEY = "-----BEGIN PRIVATE KEY-----\n";
    private static final String END_PRIVATE_KEY = "\n-----END PRIVATE KEY-----\n";

    public CertUtils()
    {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    /**
     * Create certificate signed by the provided CA.
     *
     * @param commonName                  The common name to use.
     * @param notBefore                   The notBefore to use for certificate (from).
     * @param notAfter                    The notAfter to use for certificate (to).
     * @param caCertificateFile           Path to the CA certificate file.
     * @param caCertificatePrivateKeyFile Path to the CA certificate private key file.
     * @param certificateOutputFile       The output file for the certificate in PEM format.
     * @param privateKeyOutputFile        The output file for the certificate private key in PEM format.
     */
    public void createCertificate(String commonName, Date notBefore, Date notAfter, String caCertificateFile,
            String caCertificatePrivateKeyFile, String certificateOutputFile, String privateKeyOutputFile)
    {
        try
        {
            PrivateKey caPrivateKey = getPrivateKey(Paths.get(caCertificatePrivateKeyFile).toFile());
            String caAlgorithm = caPrivateKey.getAlgorithm();

            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(caAlgorithm);
            keyPairGenerator.initialize(getKeySize(caAlgorithm));
            KeyPair keyPair = keyPairGenerator.generateKeyPair();

            X509Certificate caCertificate = getCertificate(Paths.get(caCertificateFile).toFile());
            PublicKey caPublicKey = caCertificate.getPublicKey();
            X509Certificate certificate = generate(getHashAlgorithm(caAlgorithm), keyPair.getPublic(), caPrivateKey,
                    caPublicKey, commonName, caCertificate.getSubjectDN().getName().replace("CN=", ""), notBefore,
                    notAfter, false);

            storeCertificate(certificate, Paths.get(certificateOutputFile));
            storePrivateKey(keyPair.getPrivate(), Paths.get(privateKeyOutputFile));
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Create a keystore file using the provided certificate.
     *
     * @param certFile         Path to the certificate file in PEM format.
     * @param certPrivateKey   Path to the certificate private key file in PEM format.
     * @param keyStoreType     The keystore type to use for the keystore (PKCS12 or JKS).
     * @param keyStorePassword The keystore password to use for the keystore.
     * @param outputFile       The output file where the keystore will be stored.
     */
    public void createKeyStore(String certFile, String certPrivateKey, String keyStoreType, String keyStorePassword,
            String outputFile)
    {
        try
        {
            X509Certificate certificate = getCertificate(Paths.get(certFile).toFile());
            PrivateKey privateKey = getPrivateKey(Paths.get(certPrivateKey).toFile());
            storeKeyStore(certificate, privateKey, keyStoreType, keyStorePassword, Paths.get(outputFile));
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    private PrivateKey getPrivateKey(File file) throws IOException
    {
        try(InputStream in = new FileInputStream(file))
        {
            PEMParser pemParser = new PEMParser(new InputStreamReader(in));
            Object object = pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
            PrivateKeyInfo caPrivateKeyInfo = (PrivateKeyInfo) object;
            return converter.getPrivateKey(caPrivateKeyInfo);
        }
    }

    private void storeKeyStore(X509Certificate certificate, PrivateKey privateKey, String keyStoreType,
            String keyStorePassword, Path file)
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException
    {
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, null);
        keyStore.setKeyEntry(DEFAULT_CERT_ALIAS, privateKey, keyStorePassword.toCharArray(),
                new Certificate[] { certificate });
        try(OutputStream out = new FileOutputStream(file.toFile()))
        {
            keyStore.store(out, keyStorePassword.toCharArray());
        }
    }

    /**
     * Create a truststore file using the provided certificate.
     *
     * @param certFile           Path to the certificate/CA certificate file in PEM format.
     * @param trustStoreType     The truststore type to use for the keystore (PKCS12 or JKS).
     * @param trustStorePassword The truststore password to use for the keystore.
     * @param outputFile         The output file where the truststore will be stored.
     */
    public void createTrustStore(String certFile, String trustStoreType, String trustStorePassword, String outputFile)
    {
        try
        {
            X509Certificate certificate = getCertificate(Paths.get(certFile).toFile());
            storeTrustStore(certificate, trustStoreType, trustStorePassword, Paths.get(outputFile));
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    private X509Certificate getCertificate(File file) throws IOException, CertificateException
    {
        try(InputStream in = new FileInputStream(file))
        {
            CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            X509Certificate certificate = (X509Certificate) certificateFactory.generateCertificate(in);
            return certificate;
        }
    }

    private void storeTrustStore(X509Certificate certificate, String trustStoreType, String trustStorePassword,
            Path file) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException
    {
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        trustStore.load(null, null);
        trustStore.setCertificateEntry(DEFAULT_CA_ALIAS, certificate);
        try(OutputStream out = new FileOutputStream(file.toFile()))
        {
            trustStore.store(out, trustStorePassword.toCharArray());
        }
    }

    /**
     * Create a self-signed CA certificate.
     *
     * @param commonName            The common name to use.
     * @param notBefore             The notBefore to use for certificate (from).
     * @param notAfter              The notAfter to use for certificate (to).
     * @param algorithm             The algorithm to use, supported algorithms: 'RSA' or 'EC'.
     * @param certificateOutputFile The output file for the certificate in PEM format.
     * @param privateKeyOutputFile  The output file for the certificate private key in PEM format.
     */
    public void createSelfSignedCACertificate(String commonName, Date notBefore, Date notAfter, String algorithm,
            String certificateOutputFile, String privateKeyOutputFile)
    {
        try
        {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
            keyPairGenerator.initialize(getKeySize(algorithm));
            KeyPair keyPair = keyPairGenerator.generateKeyPair();
            X509Certificate certificate = generate(getHashAlgorithm(algorithm), keyPair.getPublic(),
                    keyPair.getPrivate(), keyPair.getPublic(), commonName, commonName, notBefore, notAfter, true);
            storeCertificate(certificate, Paths.get(certificateOutputFile));
            storePrivateKey(keyPair.getPrivate(), Paths.get(privateKeyOutputFile));
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    private int getKeySize(String algorithm)
    {
        int keySize;
        if (RSA_ALGORITHM_NAME.equalsIgnoreCase(algorithm))
        {
            keySize = RSA_KEY_SIZE;
        }
        else if ("EC".equalsIgnoreCase(algorithm) || EC_ALGORITHM_NAME.equalsIgnoreCase(algorithm))
        {
            keySize = EC_KEY_SIZE;
        }
        else
        {
            throw new IllegalArgumentException("Algorithm '" + algorithm + "' is not supported.");
        }
        return keySize;
    }

    private String getHashAlgorithm(String algorithm)
    {
        String hashAlgorithm;
        if (RSA_ALGORITHM_NAME.equalsIgnoreCase(algorithm))
        {
            hashAlgorithm = RSA_HASH_ALGORITHM;
        }
        else if ("EC".equalsIgnoreCase(algorithm) || EC_ALGORITHM_NAME.equalsIgnoreCase(algorithm))
        {
            hashAlgorithm = EC_HASH_ALGORITHM;
        }
        else
        {
            throw new IllegalArgumentException("Algorithm '" + algorithm + "' is not supported.");
        }
        return hashAlgorithm;
    }

    private X509Certificate generate(String hashAlgorithm, PublicKey certificatePublicKey, PrivateKey caPrivateKey,
            PublicKey caPublicKey, String subjectCN, String issuerCN, Date notBefore, Date notAfter, boolean isCA)
            throws OperatorCreationException, CertIOException, CertificateException
    {
        Instant now = Instant.now();
        ContentSigner contentSigner = new JcaContentSignerBuilder(hashAlgorithm).build(caPrivateKey);
        X500Name subjectX500Name = new X500Name("CN=" + subjectCN);
        X500Name issuerX500Name = new X500Name("CN=" + issuerCN);
        X509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(issuerX500Name,
                BigInteger.valueOf(now.toEpochMilli()), notBefore, notAfter, subjectX500Name, certificatePublicKey);
        certificateBuilder.addExtension(Extension.subjectKeyIdentifier, false,
                        createSubjectKeyIdentifier(certificatePublicKey))
                .addExtension(Extension.authorityKeyIdentifier, false, createAuthorityKeyIdentifier(caPublicKey))
                .addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));
        return new JcaX509CertificateConverter().setProvider(new BouncyCastleProvider())
                .getCertificate(certificateBuilder.build(contentSigner));
    }

    private SubjectKeyIdentifier createSubjectKeyIdentifier(final PublicKey publicKey) throws OperatorCreationException
    {
        final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        final DigestCalculator digCalc = new BcDigestCalculatorProvider().get(
                new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

        return new X509ExtensionUtils(digCalc).createSubjectKeyIdentifier(publicKeyInfo);
    }

    private static AuthorityKeyIdentifier createAuthorityKeyIdentifier(final PublicKey publicKey)
            throws OperatorCreationException
    {
        final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        final DigestCalculator digCalc = new BcDigestCalculatorProvider().get(
                new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

        return new X509ExtensionUtils(digCalc).createAuthorityKeyIdentifier(publicKeyInfo);
    }

    private void storeCertificate(X509Certificate x509Certificate, Path file)
            throws CertificateEncodingException, IOException
    {
        String encodedCertificate = new String(
                Base64.getMimeEncoder(PEM_ENCODED_LINE_LENGTH, "\n".getBytes(StandardCharsets.UTF_8))
                        .encode(x509Certificate.getEncoded()));
        Files.write(file,
                BEGIN_CERTIFICATE.concat(encodedCertificate).concat(END_CERTIFICATE).getBytes(StandardCharsets.UTF_8));
    }

    private void storePrivateKey(PrivateKey privateKey, Path file) throws IOException
    {
        String encodedPrivateKey = new String(
                Base64.getMimeEncoder(PEM_ENCODED_LINE_LENGTH, "\n".getBytes(StandardCharsets.UTF_8))
                        .encode(privateKey.getEncoded()));
        Files.write(file,
                BEGIN_PRIVATE_KEY.concat(encodedPrivateKey).concat(END_PRIVATE_KEY).getBytes(StandardCharsets.UTF_8));
    }
}

