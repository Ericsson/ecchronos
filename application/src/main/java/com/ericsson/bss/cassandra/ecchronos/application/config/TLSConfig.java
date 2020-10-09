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

public class TLSConfig
{
    private boolean enabled;

    private String keystore;
    private String keystore_password;

    private String truststore;
    private String truststore_password;

    private String protocol;
    private String algorithm;
    private String store_type;
    private String[] cipher_suites;

    private boolean require_endpoint_verification;

    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    public String getKeystore()
    {
        return keystore;
    }

    public void setKeystore(String keystore)
    {
        this.keystore = keystore;
    }

    public String getKeystorePassword()
    {
        return keystore_password;
    }

    public void setKeystore_password(String keystore_password)
    {
        this.keystore_password = keystore_password;
    }

    public String getTruststore()
    {
        return truststore;
    }

    public void setTruststore(String truststore)
    {
        this.truststore = truststore;
    }

    public String getTruststorePassword()
    {
        return truststore_password;
    }

    public void setTruststore_password(String truststore_password)
    {
        this.truststore_password = truststore_password;
    }

    public String getProtocol()
    {
        return protocol;
    }

    public void setProtocol(String protocol)
    {
        this.protocol = protocol;
    }

    public Optional<String[]> getCipherSuites()
    {
        if (cipher_suites == null)
        {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(cipher_suites, cipher_suites.length));
    }

    public void setCipher_suites(String cipher_suites)
    {
        this.cipher_suites = transformCiphers(cipher_suites);
    }

    public Optional<String> getAlgorithm()
    {
        return Optional.ofNullable(algorithm);
    }

    public void setAlgorithm(String algorithm)
    {
        this.algorithm = algorithm;
    }

    public String getStoreType()
    {
        return store_type;
    }

    public void setStore_type(String store_type)
    {
        this.store_type = store_type;
    }

    public boolean requiresEndpointVerification()
    {
        return require_endpoint_verification;
    }

    public void setRequire_endpoint_verification(boolean require_endpoint_verification)
    {
        this.require_endpoint_verification = require_endpoint_verification;
    }

    private static String[] transformCiphers(String cipher_suites)
    {
        return cipher_suites == null ? null : cipher_suites.split(",");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TLSConfig tlsConfig = (TLSConfig) o;
        return enabled == tlsConfig.enabled &&
                require_endpoint_verification == tlsConfig.require_endpoint_verification &&
                Objects.equals(keystore, tlsConfig.keystore) &&
                Objects.equals(keystore_password, tlsConfig.keystore_password) &&
                Objects.equals(truststore, tlsConfig.truststore) &&
                Objects.equals(truststore_password, tlsConfig.truststore_password) &&
                Objects.equals(protocol, tlsConfig.protocol) &&
                Objects.equals(algorithm, tlsConfig.algorithm) &&
                Objects.equals(store_type, tlsConfig.store_type) &&
                Arrays.equals(cipher_suites, tlsConfig.cipher_suites);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(enabled, keystore, keystore_password, truststore, truststore_password, protocol,
                algorithm, store_type, require_endpoint_verification);
        result = 31 * result + Arrays.hashCode(cipher_suites);
        return result;
    }
}
