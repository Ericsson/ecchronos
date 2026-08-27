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

/** TLS configuration for JMX connections. */
public class JmxTLSConfig implements TLSConfig
{
    private final boolean myIsEnabled;
    private final String myKeyStorePath;
    private final String myKeyStorePassword;
    private final String myTrustStorePath;
    private final String myTrustStorePassword;
    private String myProtocol;
    private String myCipherSuites;
    private String[] myCipherSuitesAsList;
    private String myCertificatePath;
    private String myCertificatePrivateKeyPath;
    private String myTrustCertificatePath;
    private boolean myRequireEndpointVerification;
    private String myStoreType;
    private String myAlgorithm;
    // Since CRL is optional, make sure there always is a disabled default CRL config available.
    private CRLConfig myCRLConfig = new CRLConfig();

    /**
     * Constructs a new JmxTLSConfig.
     *
     * @param isEnabled whether TLS is enabled
     * @param keyStorePath the key store path
     * @param keyStorePassword the key store password
     * @param trustStorePath the trust store path
     * @param trustStorePassword the trust store password
     * @param certificatePath the PEM certificate file path
     * @param certificatePrivateKeyPath the PEM private key file path
     * @param trustCertificatePath the PEM trust certificate file path
     * @param algorithm the algorithm to use for key/trust manager factories
     */
    @JsonCreator
    @SuppressWarnings("CPD-START")
    public JmxTLSConfig(@JsonProperty("enabled") final boolean isEnabled,
                        @JsonProperty("keystore") final String keyStorePath,
                        @JsonProperty("keystore_password") final String keyStorePassword,
                        @JsonProperty("truststore") final String trustStorePath,
                        @JsonProperty("truststore_password") final String trustStorePassword,
                        @JsonProperty("certificate") final String certificatePath,
                        @JsonProperty("certificate_private_key") final String certificatePrivateKeyPath,
                        @JsonProperty("trust_certificate") final String trustCertificatePath,
                        @JsonProperty("algorithm") final String algorithm)
    {
        myIsEnabled = isEnabled;
        myKeyStorePath = keyStorePath;
        myKeyStorePassword = keyStorePassword;
        myTrustStorePath = trustStorePath;
        myTrustStorePassword = trustStorePassword;
        myCertificatePath = certificatePath;
        myCertificatePrivateKeyPath = certificatePrivateKeyPath;
        myTrustCertificatePath = trustCertificatePath;
        myAlgorithm = algorithm;
        if (myIsEnabled && !isKeyStoreConfigured() && !isCertificateConfigured())
        {
            throw new IllegalArgumentException(
                    "Invalid TLS config, you must either configure KeyStore or PEM based certificates.");
        }
    }
    /**
     * Constructs a new JmxTLSConfig with keystore-based configuration only.
     *
     * @param isEnabled whether TLS is enabled
     * @param keyStorePath the key store path
     * @param keyStorePassword the key store password
     * @param trustStorePath the trust store path
     * @param trustStorePassword the trust store password
     */
    @SuppressWarnings("CPD-END")
    public JmxTLSConfig(final boolean isEnabled,
                        final String keyStorePath,
                        final String keyStorePassword,
                        final String trustStorePath,
                        final String trustStorePassword
    )
    {
        myIsEnabled = isEnabled;
        myKeyStorePath = keyStorePath;
        myKeyStorePassword = keyStorePassword;
        myTrustStorePath = trustStorePath;
        myTrustStorePassword = trustStorePassword;
        if (myIsEnabled && !isKeyStoreConfigured())
        {
            throw new IllegalArgumentException("Invalid JMX TLS config, you must configure KeyStore based"
                    + " certificates.");
        }
    }
    private boolean isKeyStoreConfigured()
    {
        return myKeyStorePath != null && !myKeyStorePath.isEmpty()
                && myKeyStorePassword != null && myKeyStorePassword != null
                && myTrustStorePath != null && !myTrustStorePath.isEmpty()
                && myTrustStorePassword != null && !myTrustStorePassword.isEmpty();
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
     *
     * @return an {@link Optional} containing the certificate private key path, or empty if not configured
     */
    @Override
    public final Optional<String> getCertificatePrivateKeyPath()
    {
        return Optional.ofNullable(myCertificatePrivateKeyPath);
    }

    /**
     * Returns the trust certificate path.
     *
     * @return an {@link Optional} containing the trust certificate path, or empty if not configured
     */
    @Override
    public final Optional<String> getTrustCertificatePath()
    {
        return Optional.ofNullable(myTrustCertificatePath);
    }

    /**
     * Returns the TLS protocol version.
     *
     * @return the protocol string, or null if not set
     */
    public final String getProtocol()
    {
        return myProtocol;
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
        if (myCipherSuitesAsList == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(myCipherSuitesAsList, myCipherSuitesAsList.length));
    }

    /**
     * Returns the cipher suites as a comma-separated string.
     *
     * @return the cipher suites string, or null if not set
     */
    public final String getCipherSuitesAsString()
    {
        return myCipherSuites;
    }

    /**
     * Returns the protocols.
     * @return the protocols as a string array
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
     * Returns the CRL (Certificate Revocation List) configuration.
     *
     * @return the CRL configuration
     */
    @JsonProperty("crl")
    @Override
    public final CRLConfig getCRLConfig()
    {
        return myCRLConfig;
    }

    /**
     * Sets the cipher suites as a comma-separated string.
     *
     * @param cipherSuites the cipher suites to enable
     */
    @JsonProperty("cipher_suites")
    public final void setCipherSuites(final String cipherSuites)
    {
        myCipherSuites = cipherSuites;
        myCipherSuitesAsList = transformCiphers(cipherSuites);
    }

    private static String[] transformCiphers(final String cipherSuites)
    {
        return cipherSuites == null ? null : cipherSuites.split(",");
    }

    /**
     * Sets whether endpoint verification is required during the TLS handshake.
     *
     * @param requireEndpointVerification true to require endpoint verification
     */
    @JsonProperty("require_endpoint_verification")
    public final void setRequireEndpointVerification(final boolean requireEndpointVerification)
    {
        myRequireEndpointVerification = requireEndpointVerification;
    }

    /**
     * Sets the keystore/truststore type (e.g., "JKS", "PKCS12").
     *
     * @param storeType the store type
     */
    @JsonProperty("store_type")
    public final void setStoreType(final String storeType)
    {
        myStoreType = storeType;
    }

    /**
     * Sets the algorithm for key/trust manager factories.
     *
     * @param algorithm the algorithm name
     */
    @JsonProperty("algorithm")
    public final void setAlgorithm(final String algorithm)
    {
        myAlgorithm = algorithm;
    }

    /**
     * Sets the CRL (Certificate Revocation List) configuration.
     *
     * @param crlConfig the CRL configuration
     */
    @JsonProperty("crl")
    public final void setCRLConfig(final CRLConfig crlConfig)
    {
        myCRLConfig = crlConfig;
    }

    /** {@inheritDoc} */
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
        JmxTLSConfig that = (JmxTLSConfig) o;
        return myIsEnabled == that.myIsEnabled
                && Objects.equals(myKeyStorePath, that.myKeyStorePath)
                && Objects.equals(myKeyStorePassword, that.myKeyStorePassword)
                && Objects.equals(myTrustStorePath, that.myTrustStorePath)
                && Objects.equals(myTrustStorePassword, that.myTrustStorePassword)
                && myRequireEndpointVerification == that.myRequireEndpointVerification
                && Objects.equals(myStoreType, that.myStoreType)
                && Objects.equals(myAlgorithm, that.myAlgorithm)
                && Objects.equals(myCertificatePath, that.myCertificatePath)
                && Objects.equals(myCertificatePrivateKeyPath, that.myCertificatePrivateKeyPath)
                && Objects.equals(myTrustCertificatePath, that.myTrustCertificatePath)
                && Objects.equals(myProtocol, that.myProtocol) && Objects.equals(myCipherSuites, that.myCipherSuites);
    }

    /** {@inheritDoc} */
    @Override
    public final int hashCode()
    {
        return Objects.hash(myIsEnabled, myKeyStorePath, myKeyStorePassword, myTrustStorePath,
                myStoreType, myAlgorithm, myCertificatePath, myCertificatePrivateKeyPath,
                myTrustCertificatePath, myRequireEndpointVerification,
                myTrustStorePassword, myProtocol, myCipherSuites);
    }

    /**
     * Returns whether PEM-based certificate configuration is present.
     *
     * @return true if certificate path, private key path, and trust certificate path are all configured
     */
    @Override
    public final boolean isCertificateConfigured()
    {
        return getCertificatePath().isPresent() && getCertificatePrivateKeyPath().isPresent()
                && getTrustCertificatePath().isPresent();
    }

    /**
     * Returns whether endpoint verification is required during the TLS handshake.
     *
     * @return true if endpoint verification is required
     */
    @Override
    public final boolean requiresEndpointVerification()
    {
        return myRequireEndpointVerification;
    }

    /**
     * Returns the store type used for the keystore/truststore.
     *
     * @return an {@link Optional} containing the store type, or empty if not configured
     */
    @Override
    public final Optional<String> getStoreType()
    {
        return Optional.ofNullable(myStoreType);
    }

    /**
     * Returns the algorithm used for key/trust manager factories.
     *
     * @return an {@link Optional} containing the algorithm, or empty if not configured
     */
    @Override
    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(myAlgorithm);
    }
}
