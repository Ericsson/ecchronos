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

/** Aggregates all security-related configuration. */
public final class Security
{
    private CqlSecurity myCqlSecurity;
    private JmxSecurity myJmxSecurity;

    /** Default constructor. */
    public Security()
    {
    }

    /**
     * Returns the CQL security.
     * @return the CQL security
     */
    @JsonProperty("cql")
    public CqlSecurity getCqlSecurity()
    {
        return myCqlSecurity;
    }

    /**
     * Sets the CQL security.
     * @param cqlSecurity the CQL security
     */
    @JsonProperty("cql")
    public void setCqlSecurity(final CqlSecurity cqlSecurity)
    {
        myCqlSecurity = cqlSecurity;
    }

    /**
     * Returns the JMX security.
     * @return the JMX security
     */
    @JsonProperty("jmx")
    public JmxSecurity getJmxSecurity()
    {
        return myJmxSecurity;
    }

    /**
     * Sets the JMX security.
     * @param jmxSecurity the JMX security
     */
    @JsonProperty("jmx")
    public void setJmxSecurity(final JmxSecurity jmxSecurity)
    {
        myJmxSecurity = jmxSecurity;
    }

    /** Returns the CQL security configuration supplier. */
    public static final class CqlSecurity
    {
        private Credentials myCqlCredentials;
        private CqlTLSConfig myCqlTlsConfig;

        /** Default constructor. */
        public CqlSecurity()
        {
        }

        /**
         * Returns the CQL credentials.
         * @return the CQL credentials
         */
        @JsonProperty("credentials")
        public Credentials getCqlCredentials()
        {
            return myCqlCredentials;
        }

        /**
         * Sets the CQL credentials.
         * @param cqlCredentials the CQL credentials
         */
        @JsonProperty("credentials")
        public void setCqlCredentials(final Credentials cqlCredentials)
        {
            myCqlCredentials = cqlCredentials;
        }

        /**
         * Returns the CQL TLS config.
         * @return the CQL TLS config
         */
        @JsonProperty("tls")
        public CqlTLSConfig getCqlTlsConfig()
        {
            return myCqlTlsConfig;
        }

        /**
         * Sets the CQL TLS config.
         * @param cqlTlsConfig the CQL TLS config
         */
        @JsonProperty("tls")
        public void setCqlTlsConfig(final CqlTLSConfig cqlTlsConfig)
        {
            myCqlTlsConfig = cqlTlsConfig;
        }
    }

    /** Returns the JMX security configuration supplier. */
    public static final class JmxSecurity
    {
        private Credentials myJmxCredentials;
        private JmxTLSConfig myJmxTlsConfig;

        /** Default constructor. */
        public JmxSecurity()
        {
        }

        /**
         * Returns the JMX credentials.
         * @return the JMX credentials
         */
        @JsonProperty("credentials")
        public Credentials getJmxCredentials()
        {
            return myJmxCredentials;
        }

        /**
         * Sets the JMX credentials.
         * @param jmxCredentials the JMX credentials
         */
        @JsonProperty("credentials")
        public void setJmxCredentials(final Credentials jmxCredentials)
        {
            myJmxCredentials = jmxCredentials;
        }

        /**
         * Returns the JMX TLS config.
         * @return the JMX TLS config
         */
        @JsonProperty("tls")
        public JmxTLSConfig getJmxTlsConfig()
        {
            return myJmxTlsConfig;
        }

        /**
         * Sets the JMX TLS config.
         * @param jmxTlsConfig the JMX TLS config
         */
        @JsonProperty("tls")
        public void setJmxTlsConfig(final JmxTLSConfig jmxTlsConfig)
        {
            myJmxTlsConfig = jmxTlsConfig;
        }
    }
}
