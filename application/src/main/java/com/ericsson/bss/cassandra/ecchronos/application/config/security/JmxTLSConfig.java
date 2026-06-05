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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** TLS configuration for JMX connections. */
public class JmxTLSConfig
{
    private final boolean myIsEnabled;
    private final String myKeyStorePath;
    private final String myKeyStorePassword;
    private final String myTrustStorePath;
    private final String myTrustStorePassword;
    private String myProtocol;
    private String myCipherSuites;

    /**
     * Constructs a new JmxTLSConfig.
     * @param isEnabled whether TLS is enabled
     * @param keyStorePath the key store path
     * @param keyStorePassword the key store password
     * @param trustStorePath the trust store path
     * @param trustStorePassword the trust store password
     */
    @JsonCreator
    public JmxTLSConfig(@JsonProperty("enabled") final boolean isEnabled,
                        @JsonProperty("keystore") final String keyStorePath,
                        @JsonProperty("keystore_password") final String keyStorePassword,
                        @JsonProperty("truststore") final String trustStorePath,
                        @JsonProperty("truststore_password") final String trustStorePassword)
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
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    /**
     * Returns the key store path.
     * @return the key store path
     */
    public final String getKeyStorePath()
    {
        return myKeyStorePath;
    }

    /**
     * Returns the key store password.
     * @return the key store password
     */
    public final String getKeyStorePassword()
    {
        return myKeyStorePassword;
    }

    /**
     * Returns the trust store path.
     * @return the trust store path
     */
    public final String getTrustStorePath()
    {
        return myTrustStorePath;
    }

    /**
     * Returns the trust store password.
     * @return the trust store password
     */
    public final String getTrustStorePassword()
    {
        return myTrustStorePassword;
    }

    /**
     * Returns the protocol.
     * @return the protocol
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
    public final String getCipherSuites()
    {
        return myCipherSuites;
    }

    /**
     * Sets the cipher suites.
     * @param cipherSuites the cipher suites
     */
    @JsonProperty("cipher_suites")
    public final void setCipherSuites(final String cipherSuites)
    {
        myCipherSuites = cipherSuites;
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
                && Objects.equals(myProtocol, that.myProtocol) && Objects.equals(myCipherSuites, that.myCipherSuites);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(myIsEnabled, myKeyStorePath, myKeyStorePassword, myTrustStorePath,
                myTrustStorePassword, myProtocol, myCipherSuites);
    }
}
