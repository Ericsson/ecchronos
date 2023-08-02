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

public class JmxTLSConfig
{
    private final boolean myIsEnabled;
    private final String myKeyStorePath;
    private final String myKeyStorePassword;
    private final String myTrustStorePath;
    private final String myTrustStorePassword;
    private String myProtocol;
    private String myCipherSuites;

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

    public final String getProtocol()
    {
        return myProtocol;
    }

    @JsonProperty("protocol")
    public final void setProtocol(final String protocol)
    {
        myProtocol = protocol;
    }

    public final String getCipherSuites()
    {
        return myCipherSuites;
    }

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
