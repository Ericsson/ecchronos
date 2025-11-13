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

    @JsonCreator
    public JmxTLSConfig(@JsonProperty("enabled") final boolean isEnabled,
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
                    "Invalid TLS config, you must either configure KeyStore or PEM based certificates.");
        }
    }

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

    @Override
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    @Override
    public final String getKeyStorePath()
    {
        return myKeyStorePath;
    }

    @Override
    public final String getKeyStorePassword()
    {
        return myKeyStorePassword;
    }

    @Override
    public final String getTrustStorePath()
    {
        return myTrustStorePath;
    }

    @Override
    public final String getTrustStorePassword()
    {
        return myTrustStorePassword;
    }

    @Override
    public final Optional<String> getCertificatePath()
    {
        return Optional.ofNullable(myCertificatePath);
    }

    @Override
    public final Optional<String> getCertificatePrivateKeyPath()
    {
        return Optional.ofNullable(myCertificatePrivateKeyPath);
    }

    @Override
    public final Optional<String> getTrustCertificatePath()
    {
        return Optional.ofNullable(myTrustCertificatePath);
    }

    public final String getProtocol()
    {
        return myProtocol;
    }

    @JsonProperty("protocol")
    public final void setProtocol(final String protocol)
    {
        myProtocol = protocol;
    }

    @Override
    public final Optional<String[]> getCipherSuites()
    {
        if (myCipherSuitesAsList == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(myCipherSuitesAsList, myCipherSuitesAsList.length));
    }

    public final String getCipherSuitesAsString()
    {
        return myCipherSuites;
    }

    @Override
    public final String[] getProtocols()
    {
        if (myProtocol == null)
        {
            return null;
        }
        return myProtocol.split(",");
    }

    @JsonProperty("crl")
    @Override
    public final CRLConfig getCRLConfig()
    {
        return myCRLConfig;
    }

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

    @JsonProperty("require_endpoint_verification")
    public final void setRequireEndpointVerification(final boolean requireEndpointVerification)
    {
        myRequireEndpointVerification = requireEndpointVerification;
    }

    @JsonProperty("store_type")
    public final void setStoreType(final String storeType)
    {
        myStoreType = storeType;
    }

    @JsonProperty("algorithm")
    public final void setAlgorithm(final String algorithm)
    {
        myAlgorithm = algorithm;
    }

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

    @Override
    public final int hashCode()
    {
        return Objects.hash(myIsEnabled, myKeyStorePath, myKeyStorePassword, myTrustStorePath,
                myStoreType, myAlgorithm, myCertificatePath, myCertificatePrivateKeyPath,
                myTrustCertificatePath, myRequireEndpointVerification,
                myTrustStorePassword, myProtocol, myCipherSuites);
    }

    @Override
    public final boolean isCertificateConfigured()
    {
        return getCertificatePath().isPresent() && getCertificatePrivateKeyPath().isPresent()
                && getTrustCertificatePath().isPresent();
    }

    @Override
    public final boolean requiresEndpointVerification()
    {
        return myRequireEndpointVerification;
    }

    @Override
    public final Optional<String> getStoreType()
    {
        return Optional.ofNullable(myStoreType);
    }

    @Override
    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(myAlgorithm);
    }
}
