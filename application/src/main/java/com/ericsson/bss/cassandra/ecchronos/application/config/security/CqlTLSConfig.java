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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** TLS configuration for CQL connections. */
public class CqlTLSConfig implements TLSConfig
{
    private static final int HASH_SEED = 31;

    private final boolean myIsEnabled;
    private String myKeyStorePath;
    private String myKeyStorePassword;
    private String myTrustStorePath;
    private String myTrustStorePassword;
    private String myStoreType;
    private String myAlgorithm;
    private String myCertificatePath;
    private String myCertificatePrivateKeyPath;
    private String myTrustCertificatePath;
    private String myProtocol;
    private String[] myCipherSuites;
    private boolean myRequireEndpointVerification;
    // Since CRL is optional, make sure there always is a disabled default CRL config available.
    private CRLConfig myCRLConfig = new CRLConfig();

    /**
     * Constructs a new CqlTLSConfig.
     * @param isEnabled whether TLS is enabled
     * @param keyStorePath the key store path
     * @param keyStorePassword the key store password
     * @param trustStorePath the trust store path
     * @param trustStorePassword the trust store password
     * @param certificatePath the certificate path
     * @param certificatePrivateKeyPath the certificate private key path
     * @param trustCertificatePath the trust certificate path
     */
    @JsonCreator
    @SuppressWarnings("CPD-START")
    public CqlTLSConfig(@JsonProperty("enabled") final boolean isEnabled,
                        @JsonProperty("keystore") final String keyStorePath,
                        @JsonProperty("keystore_password") final String keyStorePassword,
                        @JsonProperty("truststore") final String trustStorePath,
                        @JsonProperty("truststore_password") final String trustStorePassword,
                        @JsonProperty("certificate") final String certificatePath,
                        @JsonProperty("certificate_private_key") final String certificatePrivateKeyPath,
                        @JsonProperty("trust_certificate") final String trustCertificatePath)
    {
        myIsEnabled = isEnabled;
        myKeyStorePath = keyStorePath;
        myKeyStorePassword = keyStorePassword;
        myTrustStorePath = trustStorePath;
        myTrustStorePassword = trustStorePassword;
        myCertificatePath = certificatePath;
        myCertificatePrivateKeyPath = certificatePrivateKeyPath;
        myTrustCertificatePath = trustCertificatePath;
        if (myIsEnabled && !isKeyStoreConfigured() && !isCertificateConfigured())
        {
            throw new IllegalArgumentException(
                    "Invalid CQL TLS config, you must either configure KeyStore or PEM based certificates.");
        }
    }
    @SuppressWarnings("CPD-END")

    private boolean isKeyStoreConfigured()
    {
        return myKeyStorePath != null && !myKeyStorePath.isEmpty()
                && myKeyStorePassword != null && myKeyStorePassword != null
                && myTrustStorePath != null && !myTrustStorePath.isEmpty()
                && myTrustStorePassword != null && !myTrustStorePassword.isEmpty();
    }

    /**
     * Returns whether certificate configured.
     * @return true if certificate configured
     */
    @Override
    public final boolean isCertificateConfigured()
    {
        return getCertificatePath().isPresent() && getCertificatePrivateKeyPath().isPresent()
                && getTrustCertificatePath().isPresent();
    }

    /**
     * Constructs a new CqlTLSConfig.
     * @param isEnabled the is enabled
     * @param keyStorePath the key store path
     * @param keyStorePassword the key store password
     * @param trustStorePath the trust store path
     * @param trustStorePassword the trust store password
     */
    public CqlTLSConfig(final boolean isEnabled, final String keyStorePath, final String keyStorePassword,
            final String trustStorePath, final String trustStorePassword)
    {
        myIsEnabled = isEnabled;
        myKeyStorePath = keyStorePath;
        myKeyStorePassword = keyStorePassword;
        myTrustStorePath = trustStorePath;
        myTrustStorePassword = trustStorePassword;
    }

    /**
     * Constructs a new CqlTLSConfig.
     * @param isEnabled the is enabled
     * @param certificatePath the certificate path
     * @param certificatePrivateKeyPath the certificate private key path
     * @param trustCertificatePath the trust certificate path
     */
    public CqlTLSConfig(final boolean isEnabled, final String certificatePath, final String certificatePrivateKeyPath,
            final String trustCertificatePath)
    {
        myIsEnabled = isEnabled;
        myCertificatePath = certificatePath;
        myCertificatePrivateKeyPath = certificatePrivateKeyPath;
        myTrustCertificatePath = trustCertificatePath;
    }

    /**
     * Returns whether enabled.
     * @return true if enabled
     */
    @Override
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    /**
     * Returns the key store path.
     * @return the key store path
     */
    @Override
    public final String getKeyStorePath()
    {
        return myKeyStorePath;
    }

    /**
     * Returns the key store password.
     * @return the key store password
     */
    @Override
    @JsonProperty(value = "keystore_password", access = JsonProperty.Access.WRITE_ONLY)
    public final String getKeyStorePassword()
    {
        return myKeyStorePassword;
    }

    /**
     * Returns the trust store path.
     * @return the trust store path
     */
    @Override
    public final String getTrustStorePath()
    {
        return myTrustStorePath;
    }

    /**
     * Returns the trust store password.
     * @return the trust store password
     */
    @Override
    @JsonProperty(value = "truststore_password", access = JsonProperty.Access.WRITE_ONLY)
    public final String getTrustStorePassword()
    {
        return myTrustStorePassword;
    }

    /**
     * Returns the store type.
     * @return the store type
     */
    @Override
    public final Optional<String> getStoreType()
    {
        return Optional.ofNullable(myStoreType);
    }

    /**
     * Sets the store type.
     * @param storeType the store type
     */
    @JsonProperty("store_type")
    public final void setStoreType(final String storeType)
    {
        myStoreType = storeType;
    }

    /**
     * Returns the algorithm.
     * @return the algorithm
     */
    @Override
    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(myAlgorithm);
    }

    /**
     * Sets the algorithm.
     * @param algorithm the encryption algorithm
     */
    @JsonProperty("algorithm")
    public final void setAlgorithm(final String algorithm)
    {
        myAlgorithm = algorithm;
    }

    /**
     * Returns the certificate path.
     * @return the certificate path
     */
    @Override
    public final Optional<String> getCertificatePath()
    {
        return Optional.ofNullable(myCertificatePath);
    }

    /**
     * Returns the certificate private key path.
     * @return the certificate private key path
     */
    @Override
    public final Optional<String> getCertificatePrivateKeyPath()
    {
        return Optional.ofNullable(myCertificatePrivateKeyPath);
    }

    /**
     * Returns the trust certificate path.
     * @return the trust certificate path
     */
    @Override
    public final Optional<String> getTrustCertificatePath()
    {
        return Optional.ofNullable(myTrustCertificatePath);
    }

    /**
     * Sets the protocol.
     * @param protocol the TLS protocol version
     */
    @JsonProperty("protocol")
    public final void setProtocol(final String protocol)
    {
        myProtocol = protocol;
    }

    /**
     * Returns the cipher suites.
     * @return the cipher suites
     */
    @Override
    public final Optional<String[]> getCipherSuites()
    {
        if (myCipherSuites == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(myCipherSuites, myCipherSuites.length));
    }

    /**
     * Sets the cipher suites.
     * @param cipherSuites the cipher suites
     */
    @JsonProperty("cipher_suites")
    public final void setCipherSuites(final String cipherSuites)
    {
        myCipherSuites = transformCiphers(cipherSuites);
    }

    private static String[] transformCiphers(final String cipherSuites)
    {
        return cipherSuites == null ? null : cipherSuites.split(",");
    }

    /**
     * Returns the protocols.
     * @return the protocols
     */
    @Override
    public final String[] getProtocols()
    {
        if (myProtocol == null)
        {
            return null;
        }
        return myProtocol.split(",");
    }

    /**
     * Returns whether endpoint verification is required.
     * @return true if endpoint verification is required
     */
    @Override
    public final boolean requiresEndpointVerification()
    {
        return myRequireEndpointVerification;
    }

    /**
     * Sets the require endpoint verification.
     * @param requireEndpointVerification the require endpoint verification
     */
    @JsonProperty("require_endpoint_verification")
    public final void setRequireEndpointVerification(final boolean requireEndpointVerification)
    {
        myRequireEndpointVerification = requireEndpointVerification;
    }

    /**
     * Returns the CRL config.
     * @return the CRL config
     */
    @JsonProperty("crl")
    @Override
    public final CRLConfig getCRLConfig()
    {
        return myCRLConfig;
    }

    /**
     * Sets the CRL config.
     * @param crlConfig the CRL config
     */
    @JsonProperty("crl")
    public final void setCRLConfig(final CRLConfig crlConfig)
    {
        myCRLConfig = crlConfig;
    }
    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        CqlTLSConfig that = (CqlTLSConfig) o;
        return myIsEnabled == that.myIsEnabled && myRequireEndpointVerification == that.myRequireEndpointVerification
                && Objects.equals(myKeyStorePath, that.myKeyStorePath)
                && Objects.equals(myKeyStorePassword, that.myKeyStorePassword)
                && Objects.equals(myTrustStorePath, that.myTrustStorePath)
                && Objects.equals(myTrustStorePassword, that.myTrustStorePassword)
                && Objects.equals(myStoreType, that.myStoreType)
                && Objects.equals(myAlgorithm, that.myAlgorithm)
                && Objects.equals(myCertificatePath, that.myCertificatePath)
                && Objects.equals(myCertificatePrivateKeyPath, that.myCertificatePrivateKeyPath)
                && Objects.equals(myTrustCertificatePath, that.myTrustCertificatePath)
                && Objects.equals(myProtocol, that.myProtocol) && Arrays.equals(myCipherSuites, that.myCipherSuites);
    }

    @Override
    public final int hashCode()
    {
        int result = Objects.hash(myIsEnabled, myKeyStorePath, myKeyStorePassword, myTrustStorePath,
                myTrustStorePassword,
                myStoreType, myAlgorithm, myCertificatePath, myCertificatePrivateKeyPath, myTrustCertificatePath,
                myProtocol, myRequireEndpointVerification);
        result = HASH_SEED * result + Arrays.hashCode(myCipherSuites);
        return result;
    }
}
