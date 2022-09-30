/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;


@SuppressWarnings({"checkstyle:membername", "checkstyle:methodname"})
public class TLSConfig
{
    private static final int HASH_SEED = 31;

    private boolean enabled;

    private String keystore;
    private String keystore_password;

    private String truststore;
    private String truststore_password;

    private String certificate;
    private String certificate_private_key;
    private String trust_certificate;

    private String protocol;
    private String algorithm;
    private String store_type;
    private String[] cipher_suites;

    private boolean require_endpoint_verification;

    public final boolean isEnabled()
    {
        return enabled;
    }

    public final void setEnabled(final boolean isEnabled)
    {
        this.enabled = isEnabled;
    }

    public final String getKeystore()
    {
        return keystore;
    }

    public final void setKeystore(final String aKeystore)
    {
        this.keystore = aKeystore;
    }

    public final String getKeystore_password()
    {
        return keystore_password;
    }

    public final void setKeystore_password(final String keystorePassword)
    {
        this.keystore_password = keystorePassword;
    }

    public final String getTruststore()
    {
        return truststore;
    }

    public final void setTruststore(final String aTruststore)
    {
        this.truststore = aTruststore;
    }

    public final String getTruststore_password()
    {
        return truststore_password;
    }

    public final void setTruststore_password(final String truststorePassword)
    {
        this.truststore_password = truststorePassword;
    }

    public final Optional<String> getCertificate()
    {
        return Optional.ofNullable(certificate);
    }

    public final void setCertificate(final String aCertificate)
    {
        this.certificate = aCertificate;
    }

    public final Optional<String> getCertificatePrivateKey()
    {
        return Optional.ofNullable(certificate_private_key);
    }

    public final void setCertificate_private_key(final String certificatePrivateKey)
    {
        this.certificate_private_key = certificatePrivateKey;
    }

    public final Optional<String> getTrustCertificate()
    {
        return Optional.ofNullable(trust_certificate);
    }

    public final void setTrust_certificate(final String trustCertificate)
    {
        this.trust_certificate = trustCertificate;
    }

    public final String getProtocol()
    {
        return protocol;
    }

    public final String[] getProtocols()
    {
        return protocol.split(",");
    }

    public final void setProtocol(final String aProtocol)
    {
        this.protocol = aProtocol;
    }

    public final Optional<String[]> getCipher_suites()
    {
        if (cipher_suites == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(cipher_suites, cipher_suites.length));
    }

    public final void setCipher_suites(final String cipherSuites)
    {
        this.cipher_suites = transformCiphers(cipherSuites);
    }

    public final Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(algorithm);
    }

    public final void setAlgorithm(final String theAlgorithm)
    {
        this.algorithm = theAlgorithm;
    }

    public final String getStore_type()
    {
        return store_type;
    }

    public final void setStore_type(final String storeType)
    {
        this.store_type = storeType;
    }

    public final boolean requiresEndpointVerification()
    {
        return require_endpoint_verification;
    }

    public final void setRequire_endpoint_verification(final boolean requireEndpointVerification)
    {
        this.require_endpoint_verification = requireEndpointVerification;
    }

    private static String[] transformCiphers(final String cipherSuites)
    {
        return cipherSuites == null ? null : cipherSuites.split(",");
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
        return enabled == tlsConfig.enabled
                && require_endpoint_verification == tlsConfig.require_endpoint_verification
                && Objects.equals(keystore, tlsConfig.keystore)
                && Objects.equals(keystore_password, tlsConfig.keystore_password)
                && Objects.equals(truststore, tlsConfig.truststore)
                && Objects.equals(truststore_password, tlsConfig.truststore_password)
                && Objects.equals(certificate, tlsConfig.certificate)
                && Objects.equals(certificate_private_key, tlsConfig.certificate_private_key)
                && Objects.equals(trust_certificate, tlsConfig.trust_certificate)
                && Objects.equals(protocol, tlsConfig.protocol)
                && Objects.equals(algorithm, tlsConfig.algorithm)
                && Objects.equals(store_type, tlsConfig.store_type)
                && Arrays.equals(cipher_suites, tlsConfig.cipher_suites);
    }

    @Override
    public final int hashCode()
    {
        int result = Objects.hash(enabled, keystore, keystore_password, truststore, truststore_password, certificate,
                certificate_private_key, trust_certificate, protocol, algorithm, store_type,
                require_endpoint_verification);
        result = HASH_SEED * result + Arrays.hashCode(cipher_suites);
        return result;
    }
}
