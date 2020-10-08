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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.Credentials;
import com.ericsson.bss.cassandra.ecchronos.application.config.Security;
import com.ericsson.bss.cassandra.ecchronos.application.config.TLSConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalJmxConnectionProvider;
import com.google.common.base.Joiner;

public class DefaultJmxConnectionProvider implements JmxConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultJmxConnectionProvider.class);

    private final LocalJmxConnectionProvider myLocalJmxConnectionProvider;

    public DefaultJmxConnectionProvider(Config config, Supplier<Security.JmxSecurity> jmxSecurity) throws IOException
    {
        Config.Connection<JmxConnectionProvider> jmxConfig = config.getConnectionConfig().getJmx();
        String host = jmxConfig.getHost();
        int port = jmxConfig.getPort();
        boolean authEnabled = jmxSecurity.get().getCredentials().isEnabled();
        boolean tlsEnabled = jmxSecurity.get().getTls().isEnabled();
        LOG.info("Connecting through JMX using {}:{}, authentication: {}, tls: {}", host, port, authEnabled,
                tlsEnabled);

        Supplier<String[]> credentials = () -> convertCredentials(jmxSecurity);
        Supplier<Map<String, String>> tls = () -> convertTls(jmxSecurity);

        myLocalJmxConnectionProvider = new LocalJmxConnectionProvider(host, port, credentials, tls);
    }

    @Override
    public JMXConnector getJmxConnector() throws IOException
    {
        return myLocalJmxConnectionProvider.getJmxConnector();
    }

    @Override
    public void close() throws IOException
    {
        myLocalJmxConnectionProvider.close();
    }

    private Map<String, String> convertTls(Supplier<Security.JmxSecurity> jmxSecurity)
    {
        TLSConfig tlsConfig = jmxSecurity.get().getTls();
        if (!tlsConfig.isEnabled())
        {
            return new HashMap<>();
        }

        Map<String, String> config = new HashMap<>();
        config.put("com.sun.management.jmxremote.ssl.enabled.protocols", tlsConfig.getProtocol());
        String ciphers = tlsConfig.getCipherSuites()
                .map(Joiner.on(',')::join)
                .orElse("");
        config.put("com.sun.management.jmxremote.ssl.enabled.cipher.suites", ciphers);

        config.put("javax.net.ssl.keyStore", tlsConfig.getKeystore());
        config.put("javax.net.ssl.keyStorePassword", tlsConfig.getKeystorePassword());
        config.put("javax.net.ssl.trustStore", tlsConfig.getTruststore());
        config.put("javax.net.ssl.trustStorePassword", tlsConfig.getTruststorePassword());

        return config;
    }

    private String[] convertCredentials(Supplier<Security.JmxSecurity> jmxSecurity)
    {
        Credentials credentials = jmxSecurity.get().getCredentials();
        if (!credentials.isEnabled())
        {
            return null;
        }
        return new String[] { credentials.getUsername(), credentials.getPassword() };
    }
}
