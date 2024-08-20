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

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Security
{
    private CqlSecurity myCqlSecurity;
    private JmxSecurity myJmxSecurity;

    @JsonProperty("cql")
    public CqlSecurity getCqlSecurity()
    {
        return myCqlSecurity;
    }

    @JsonProperty("cql")
    public void setCqlSecurity(final CqlSecurity cqlSecurity)
    {
        myCqlSecurity = cqlSecurity;
    }

    @JsonProperty("jmx")
    public JmxSecurity getJmxSecurity()
    {
        return myJmxSecurity;
    }

    @JsonProperty("jmx")
    public void setJmxSecurity(final JmxSecurity jmxSecurity)
    {
        myJmxSecurity = jmxSecurity;
    }

    public static final class CqlSecurity
    {
        private Credentials myCqlCredentials;
        private CqlTLSConfig myCqlTlsConfig;

        @JsonProperty("credentials")
        public Credentials getCqlCredentials()
        {
            return myCqlCredentials;
        }

        @JsonProperty("credentials")
        public void setCqlCredentials(final Credentials cqlCredentials)
        {
            myCqlCredentials = cqlCredentials;
        }

        @JsonProperty("tls")
        public CqlTLSConfig getCqlTlsConfig()
        {
            return myCqlTlsConfig;
        }

        @JsonProperty("tls")
        public void setCqlTlsConfig(final CqlTLSConfig cqlTlsConfig)
        {
            myCqlTlsConfig = cqlTlsConfig;
        }
    }

    public static final class JmxSecurity
    {
        private Credentials myJmxCredentials;
        private JmxTLSConfig myJmxTlsConfig;

        @JsonProperty("credentials")
        public Credentials getJmxCredentials()
        {
            return myJmxCredentials;
        }

        @JsonProperty("credentials")
        public void setJmxCredentials(final Credentials jmxCredentials)
        {
            myJmxCredentials = jmxCredentials;
        }

        @JsonProperty("tls")
        public JmxTLSConfig getJmxTlsConfig()
        {
            return myJmxTlsConfig;
        }

        @JsonProperty("tls")
        public void setJmxTlsConfig(final JmxTLSConfig jmxTlsConfig)
        {
            myJmxTlsConfig = jmxTlsConfig;
        }
    }
}
