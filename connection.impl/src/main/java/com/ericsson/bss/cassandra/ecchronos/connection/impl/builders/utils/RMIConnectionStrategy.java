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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy;

public final class RMIConnectionStrategy implements JmxConnectionStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(RMIConnectionStrategy.class);
    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
    private static final int DEFAULT_PORT = 7199;
    private final ConnectionUtils myConnectionUtils;
    private final int myPort;

    public RMIConnectionStrategy(final Builder builder)
    {
        myConnectionUtils = builder.myConnectionUtils;
        myPort = builder.myPort;
    }

    /**
     * Creates an RMI JMX connection to the given node.
     *
     * @param node the Cassandra node to connect to.
     * @return a ConnectionResult containing the JMXConnector and service URL.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public ConnectionResult connect(final Node node) throws IOException
    {
        String host = myConnectionUtils.defineHost(node);
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_FORMAT_URL, host, myPort));
        LOG.info("Starting to instantiate JMXService with host: {} and port: {}", host, myPort);
        JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxUrl, createJMXEnv());
        LOG.debug("Connecting JMXConnector through {}, credentials: {}, tls: {}", jmxUrl, myConnectionUtils.isAuthEnabled(), myConnectionUtils.isTLSEnabled());
        return new ConnectionResult(jmxConnector, jmxUrl);
    }

    private Map<String, Object> createJMXEnv()
    {
        Map<String, Object> env = new HashMap<>();
        String[] credentials = myConnectionUtils.getCredentialsConfig();
        Map<String, String> tls = myConnectionUtils.getTLSConfig();
        if (credentials != null)
        {
            env.put(JMXConnector.CREDENTIALS, credentials);
        }
        if (!tls.isEmpty())
        {
            env = setRmiTlsConfig(env, tls);
        }
        return env;
    }

    private Map<String, Object> setRmiTlsConfig(final Map<String, Object> env, final Map<String, String> tls)
    {
        for (Map.Entry<String, String> configEntry : tls.entrySet())
        {
            myConnectionUtils.setSystemPropertyIfNotNull(configEntry.getKey(), configEntry.getValue());
        }
        env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
        return env;
    }

    /**
     * Creates a new Builder instance.
     *
     * @return Builder
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * Builder for constructing RMIConnectionUtils.
     */
    public static final class Builder
    {
        private ConnectionUtils myConnectionUtils;
        private int myPort = DEFAULT_PORT;

        /**
         * Set the connection utils.
         *
         * @param connectionUtils the connection utils.
         * @return Builder
         */
        public Builder withConnectionUtils(final ConnectionUtils connectionUtils)
        {
            myConnectionUtils = connectionUtils;
            return this;
        }

        public Builder withPort(final int port)
        {
            myPort = port;
            return this;
        }

        /**
         * Build the RMIConnectionUtils instance.
         *
         * @return RMIConnectionUtils
         */
        public RMIConnectionStrategy build()
        {
            return new RMIConnectionStrategy(this);
        }
    }
}
