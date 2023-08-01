/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;


public class TLSConfig
{
    private static final int HASH_SEED = 31;

    private boolean myIsEnabled;
    private String myKeyStorePath;
    private String myKeyStorePassword;
    private String myTrustStorePath;
    private String myTrustStorePassword;
    private String myCertificatePath;
    private String myCertificatePrivateKeyPath;
    private String myTrustCertificatePath;
    private String myProtocol;
    private String myAlgorithm;
    private String myStoreType;
    private String[] myCipherSuites;
    private boolean myRequireEndpointVerification;

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    @JsonProperty("keystore")
    public final String getKeyStorePath()
    {
        return myKeyStorePath;
    }

    @JsonProperty("keystore")
    public final void setKeyStorePath(final String keyStorePath)
    {
        myKeyStorePath = keyStorePath;
    }

    @JsonProperty("keystore_password")
    public final String getKeyStorePassword()
    {
        return myKeyStorePassword;
    }

    @JsonProperty("keystore_password")
    public final void setKeyStorePassword(final String keyStorePassword)
    {
        myKeyStorePassword = keyStorePassword;
    }

    @JsonProperty("truststore")
    public final String getTrustStorePath()
    {
        return myTrustStorePath;
    }

    @JsonProperty("truststore")
    public final void setTrustStorePath(final String trustStorePath)
    {
        myTrustStorePath = trustStorePath;
    }

    @JsonProperty("truststore_password")
    public final String getTrustStorePassword()
    {
        return myTrustStorePassword;
    }

    @JsonProperty("truststore_password")
    public final void setTrustStorePassword(final String trustStorePassword)
    {
        myTrustStorePassword = trustStorePassword;
    }

    @JsonProperty("certificate")
    public final Optional<String> getCertificatePath()
    {
        return Optional.ofNullable(myCertificatePath);
    }

    @JsonProperty("certificate")
    public final void setCertificatePath(final String certificatePath)
    {
        myCertificatePath = certificatePath;
    }

    @JsonProperty("certificate_private_key")
    public final Optional<String> getCertificatePrivateKeyPath()
    {
        return Optional.ofNullable(myCertificatePrivateKeyPath);
    }

    @JsonProperty("certificate_private_key")
    public final void setCertificatePrivateKeyPath(final String certificatePrivateKeyPath)
    {
        myCertificatePrivateKeyPath = certificatePrivateKeyPath;
    }

    @JsonProperty("trust_certificate")
    public final Optional<String> getTrustCertificatePath()
    {
        return Optional.ofNullable(myTrustCertificatePath);
    }

    @JsonProperty("trust_certificate")
    public final void setTrustCertificatePath(final String trustCertificatePath)
    {
        myTrustCertificatePath = trustCertificatePath;
    }

    @JsonProperty("protocol")
    public final String getProtocol()
    {
        return myProtocol;
    }

    @JsonProperty("protocol")
    public final void setProtocol(final String protocol)
    {
        myProtocol = protocol;
    }

    @JsonProperty("cipher_suites")
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

    @JsonProperty("algorithm")
    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(myAlgorithm);
    }

    @JsonProperty("algorithm")
    public final void setAlgorithm(final String algorithm)
    {
        myAlgorithm = algorithm;
    }

    @JsonProperty("store_type")
    public final String getStoreType()
    {
        return myStoreType;
    }

    @JsonProperty("store_type")
    public final void setStoreType(final String storeType)
    {
        myStoreType = storeType;
    }

    @JsonProperty("require_endpoint_verification")
    public final boolean requiresEndpointVerification()
    {
        return myRequireEndpointVerification;
    }

    @JsonProperty("require_endpoint_verification")
    public final void setRequireEndpointVerification(final boolean requireEndpointVerification)
    {
        myRequireEndpointVerification = requireEndpointVerification;
    }

    public final String[] getProtocols()
    {
        return myProtocol.split(",");
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
        TLSConfig tlsConfig = (TLSConfig) o;
        return myIsEnabled == tlsConfig.myIsEnabled
                && myRequireEndpointVerification == tlsConfig.myRequireEndpointVerification
                && Objects.equals(myKeyStorePath, tlsConfig.myKeyStorePath)
                && Objects.equals(myKeyStorePassword, tlsConfig.myKeyStorePassword)
                && Objects.equals(myTrustStorePath, tlsConfig.myTrustStorePath)
                && Objects.equals(myTrustStorePassword, tlsConfig.myTrustStorePassword)
                && Objects.equals(myCertificatePath, tlsConfig.myCertificatePath)
                && Objects.equals(myCertificatePrivateKeyPath, tlsConfig.myCertificatePrivateKeyPath)
                && Objects.equals(myTrustCertificatePath, tlsConfig.myTrustCertificatePath)
                && Objects.equals(myProtocol, tlsConfig.myProtocol)
                && Objects.equals(myAlgorithm, tlsConfig.myAlgorithm)
                && Objects.equals(myStoreType, tlsConfig.myStoreType)
                && Arrays.equals(myCipherSuites, tlsConfig.myCipherSuites);
    }

    @Override
    public final int hashCode()
    {
        int result = Objects.hash(myIsEnabled, myKeyStorePath, myKeyStorePassword, myTrustStorePath,
                myTrustStorePassword, myCertificatePath, myCertificatePrivateKeyPath, myTrustCertificatePath,
                myProtocol, myAlgorithm, myStoreType, myRequireEndpointVerification);
        result = HASH_SEED * result + Arrays.hashCode(myCipherSuites);
        return result;
    }
}
