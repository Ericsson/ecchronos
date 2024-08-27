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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;

public class LocalJmxConnectionProvider implements JmxConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(LocalJmxConnectionProvider.class);

    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    public static final int DEFAULT_PORT = 7199;
    public static final String DEFAULT_HOST = "localhost";

    private final AtomicReference<JMXConnector> myJmxConnection = new AtomicReference<>();

    private final String myLocalhost;
    private final int myPort;
    private final Supplier<String[]> credentialsSupplier;
    private final Supplier<Map<String, String>> tlsSupplier;

    public LocalJmxConnectionProvider(final String localhost, final int port) throws IOException
    {
        this(localhost, port, () -> null, HashMap::new);
    }

    public LocalJmxConnectionProvider(final String localhost,
                                      final int port,
                                      final Supplier<String[]> aCredentialsSupplier,
                                      final Supplier<Map<String, String>> aTLSSupplier)
            throws IOException
    {
        myLocalhost = localhost;
        myPort = port;
        this.credentialsSupplier = aCredentialsSupplier;
        this.tlsSupplier = aTLSSupplier;

        reconnect();
    }

    @Override
    public final JMXConnector getJmxConnector() throws IOException
    {
        JMXConnector jmxConnector = myJmxConnection.get();

        if (jmxConnector == null || !isConnected(jmxConnector))
        {
            reconnect();
            return getJmxConnector();
        }

        return jmxConnector;
    }

    @Override
    public final void close() throws IOException
    {
        switchJmxConnection(null);
    }

    private void reconnect() throws IOException
    {
        String host = myLocalhost;
        if (host.contains(":"))
        {
            // Use square brackets to surround IPv6 addresses
            host = "[" + host + "]";
        }
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_FORMAT_URL, host, myPort));
        Map<String, Object> env = new HashMap<>();
        String[] credentials = this.credentialsSupplier.get();
        if (credentials != null)
        {
            env.put(JMXConnector.CREDENTIALS, credentials);
        }
        Map<String, String> tls = this.tlsSupplier.get();
        if (!tls.isEmpty())
        {
            for (Map.Entry<String, String> configEntry : tls.entrySet())
            {
                String key = configEntry.getKey();
                String value = configEntry.getValue();

                if (!value.isEmpty())
                {
                    System.setProperty(key, value);
                }
                else
                {
                    System.clearProperty(key);
                }
            }
            env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
        }

        boolean authEnabled = credentials != null;
        boolean tlsEnabled = !tls.isEmpty();

        JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, env);
        LOG.debug("Connected JMX for {}, credentials: {}, tls: {}", jmxUrl, authEnabled, tlsEnabled);

        switchJmxConnection(jmxConnector);
    }

    private void switchJmxConnection(final JMXConnector newJmxConnector) throws IOException
    {
        JMXConnector oldJmxConnector = myJmxConnection.getAndSet(newJmxConnector);

        if (oldJmxConnector != null)
        {
            oldJmxConnector.close();
        }
    }

    private static boolean isConnected(final JMXConnector jmxConnector)
    {
        try
        {
            jmxConnector.getConnectionId();
        }
        catch (IOException e)
        {
            return false;
        }

        return true;
    }
}
