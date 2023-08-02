/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.management.remote.JMXConnector;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.Connection;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.JmxTLSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalJmxConnectionProvider;

public class DefaultJmxConnectionProvider implements JmxConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJmxConnectionProvider.class);

    private final LocalJmxConnectionProvider myLocalJmxConnectionProvider;

    public DefaultJmxConnectionProvider(final Config config,
                                        final Supplier<Security.JmxSecurity> jmxSecurity) throws IOException
    {
        Connection<JmxConnectionProvider> jmxConfig = config.getConnectionConfig().getJmxConnection();
        String host = jmxConfig.getHost();
        int port = jmxConfig.getPort();
        boolean authEnabled = jmxSecurity.get().getJmxCredentials().isEnabled();
        boolean tlsEnabled = jmxSecurity.get().getJmxTlsConfig().isEnabled();
        LOG.info("Connecting through JMX using {}:{}, authentication: {}, tls: {}", host, port, authEnabled,
                tlsEnabled);

        Supplier<String[]> credentials = () -> convertCredentials(jmxSecurity);
        Supplier<Map<String, String>> tls = () -> convertTls(jmxSecurity);

        myLocalJmxConnectionProvider = new LocalJmxConnectionProvider(host, port, credentials, tls);
    }

    @Override
    public final JMXConnector getJmxConnector() throws IOException
    {
        return myLocalJmxConnectionProvider.getJmxConnector();
    }

    @Override
    public final void close() throws IOException
    {
        myLocalJmxConnectionProvider.close();
    }

    private Map<String, String> convertTls(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        JmxTLSConfig tlsConfig = jmxSecurity.get().getJmxTlsConfig();
        if (!tlsConfig.isEnabled())
        {
            return new HashMap<>();
        }

        Map<String, String> config = new HashMap<>();
        if (tlsConfig.getProtocol() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.protocols", tlsConfig.getProtocol());
        }
        if (tlsConfig.getCipherSuites() != null)
        {
            config.put("com.sun.management.jmxremote.ssl.enabled.cipher.suites", tlsConfig.getCipherSuites());
        }
        config.put("javax.net.ssl.keyStore", tlsConfig.getKeyStorePath());
        config.put("javax.net.ssl.keyStorePassword", tlsConfig.getKeyStorePassword());
        config.put("javax.net.ssl.trustStore", tlsConfig.getTrustStorePath());
        config.put("javax.net.ssl.trustStorePassword", tlsConfig.getTrustStorePassword());

        return config;
    }

    private String[] convertCredentials(final Supplier<Security.JmxSecurity> jmxSecurity)
    {
        Credentials credentials = jmxSecurity.get().getJmxCredentials();
        if (!credentials.isEnabled())
        {
            return null;
        }
        return new String[] {
                credentials.getUsername(), credentials.getPassword()
        };
    }
}
