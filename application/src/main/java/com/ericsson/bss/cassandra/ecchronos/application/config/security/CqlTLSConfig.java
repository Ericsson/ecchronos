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

public class CqlTLSConfig
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

    @JsonCreator
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
            throw new IllegalArgumentException("Invalid CQL TLS config, you must either configure KeyStore or PEM based"
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

    public final boolean isCertificateConfigured()
    {
        return getCertificatePath().isPresent() && getCertificatePrivateKeyPath().isPresent()
                && getTrustCertificatePath().isPresent();
    }

    public CqlTLSConfig(final boolean isEnabled, final String keyStorePath, final String keyStorePassword,
            final String trustStorePath, final String trustStorePassword)
    {
        myIsEnabled = isEnabled;
        myKeyStorePath = keyStorePath;
        myKeyStorePassword = keyStorePassword;
        myTrustStorePath = trustStorePath;
        myTrustStorePassword = trustStorePassword;
    }

    public CqlTLSConfig(final boolean isEnabled, final String certificatePath, final String certificatePrivateKeyPath,
            final String trustCertificatePath)
    {
        myIsEnabled = isEnabled;
        myCertificatePath = certificatePath;
        myCertificatePrivateKeyPath = certificatePrivateKeyPath;
        myTrustCertificatePath = trustCertificatePath;
    }

    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    public final String getKeyStorePath()
    {
        return myKeyStorePath;
    }

    public final String getKeyStorePassword()
    {
        return myKeyStorePassword;
    }

    public final String getTrustStorePath()
    {
        return myTrustStorePath;
    }

    public final String getTrustStorePassword()
    {
        return myTrustStorePassword;
    }

    public final Optional<String> getStoreType()
    {
        return Optional.ofNullable(myStoreType);
    }

    @JsonProperty("store_type")
    public final void setStoreType(final String storeType)
    {
        myStoreType = storeType;
    }

    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(myAlgorithm);
    }

    @JsonProperty("algorithm")
    public final void setAlgorithm(final String algorithm)
    {
        myAlgorithm = algorithm;
    }

    public final Optional<String> getCertificatePath()
    {
        return Optional.ofNullable(myCertificatePath);
    }

    public final Optional<String> getCertificatePrivateKeyPath()
    {
        return Optional.ofNullable(myCertificatePrivateKeyPath);
    }

    public final Optional<String> getTrustCertificatePath()
    {
        return Optional.ofNullable(myTrustCertificatePath);
    }

    @JsonProperty("protocol")
    public final void setProtocol(final String protocol)
    {
        myProtocol = protocol;
    }

    public final Optional<String[]> getCipherSuites()
    {
        if (myCipherSuites == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(myCipherSuites, myCipherSuites.length));
    }

    @JsonProperty("cipher_suites")
    public final void setCipherSuites(final String cipherSuites)
    {
        myCipherSuites = transformCiphers(cipherSuites);
    }

    private static String[] transformCiphers(final String cipherSuites)
    {
        return cipherSuites == null ? null : cipherSuites.split(",");
    }

    public final String[] getProtocols()
    {
        if (myProtocol == null)
        {
            return null;
        }
        return myProtocol.split(",");
    }

    public final boolean requiresEndpointVerification()
    {
        return myRequireEndpointVerification;
    }

    @JsonProperty("require_endpoint_verification")
    public final void setRequireEndpointVerification(final boolean requireEndpointVerification)
    {
        myRequireEndpointVerification = requireEndpointVerification;
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
