/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import org.jolokia.client.jmxadapter.JolokiaJmxConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class JolokiaJmxConnectionFactory implements JmxConnectionFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(JolokiaJmxConnectionFactory.class);
    private static final String JMX_JOLOKIA_FORMAT_URL = "service:jmx:%s://%s:%d/jolokia/";
    static final int CONNECTION_TIMEOUT_SECONDS = 20;

    private final boolean mySslEnabled;

    public JolokiaJmxConnectionFactory(final boolean sslEnabled)
    {
        mySslEnabled = sslEnabled;
    }

    @Override
    public JMXConnector connect(final String host, final int port, final Map<String, Object> env) throws IOException
    {
        String protocol = mySslEnabled ? "jolokia+https" : "jolokia";
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_JOLOKIA_FORMAT_URL, protocol, host, port));
        LOG.info("Creating Jolokia JMXConnection with host: {} and port: {}", host, port);

        JolokiaJmxConnectionProvider provider = new JolokiaJmxConnectionProvider();
        JMXConnector jmxConnector = provider.newJMXConnector(jmxUrl, env);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try
        {
            Future<?> future = exec.submit(() ->
            {
                try
                {
                    jmxConnector.connect();
                }
                catch (IOException e)
                {
                    LOG.error("Jolokia connection IOException during connect()", e);
                    throw new IllegalStateException("Failed to connect to Jolokia", e);
                }
            });
            future.get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e)
        {
            LOG.error("Jolokia connection failed with timeout or execution error", e);
            closeQuietly(jmxConnector);
            throw new IOException("Jolokia connection failed", e);
        }
        finally
        {
            exec.shutdownNow();
        }

        if (jmxConnector.getMBeanServerConnection() == null)
        {
            closeQuietly(jmxConnector);
            throw new IOException("MBeanServerConnection is null after Jolokia connection");
        }

        return jmxConnector;
    }

    private static void closeQuietly(final JMXConnector connector)
    {
        try
        {
            connector.close();
        }
        catch (IOException | NullPointerException e)
        {
            LOG.debug("Failed to close JMX connector", e);
        }
    }
}
