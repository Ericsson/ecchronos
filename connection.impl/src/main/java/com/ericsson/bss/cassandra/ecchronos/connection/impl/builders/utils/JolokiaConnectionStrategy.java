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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import org.jolokia.client.jmxadapter.JolokiaJmxConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy;

public final class JolokiaConnectionStrategy implements JmxConnectionStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(JolokiaConnectionStrategy.class);
    private static final String JMX_JOLOKIA_FORMAT_URL = "service:jmx:%s://%s:%d/jolokia/";
    public static final String JOLOKIA_CA_CERTIFICATE_PROPERTY = "jolokia.caCertificate";
    public static final String JOLOKIA_CLIENT_CERTIFICATE_PROPERTY = "jolokia.clientCertificate";
    public static final String JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY = "jolokia.clientKey";
    public static final String JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY = "jolokia.clientKeyAlgorithm";
    public static final String JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY = "jdk.internal.httpclient.disableHostnameVerification";
    public static final String ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY = "ecchronos.jolokia.ssl.enabled";
    public static final String JOLOKIA_HTTPS_PROTOCOL = "jolokia+https";
    public static final String JOLOKIA_PROTOCOL = "jolokia";
    private final ConnectionUtils myConnectionUtils;
    private final int myPort;
    private static final int DEFAULT_JOLOKIA_PORT = 8778;

    public JolokiaConnectionStrategy(final Builder builder)
    {
        myConnectionUtils = builder.myConnectionUtils;
        myPort = builder.myPort;
    }

    /**
     * Creates a Jolokia JMX connection to the given node.
     *
     * @param node the Cassandra node to connect to.
     * @return a ConnectionResult containing the JMXConnector and service URL.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public ConnectionResult connect(final Node node) throws IOException
    {
        String host = myConnectionUtils.defineHost(node);
        String protocol = String.valueOf(true).equals(myConnectionUtils.getTLSConfig().get(ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY)) ? JOLOKIA_HTTPS_PROTOCOL : JOLOKIA_PROTOCOL;
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_JOLOKIA_FORMAT_URL, protocol, host, myPort));
        JolokiaJmxConnectionProvider jolokiaJmxConnectionProvider = new JolokiaJmxConnectionProvider();
        LOG.info("Creating Jolokia JMXConnection with host: {} and port: {}", host, myPort);
        JMXConnector jmxConnector = jolokiaJmxConnectionProvider.newJMXConnector(jmxUrl, createJMXEnv());
        LOG.debug("Connecting JolokiaJMXConnector through {}, credentials: {}, tls: {}", jmxUrl, myConnectionUtils.isAuthEnabled(), myConnectionUtils.isTLSEnabled());
        return new ConnectionResult(tryConnection(jmxConnector), jmxUrl);
    }

    private JMXConnector tryConnection(final JMXConnector jmxConnector) throws IOException
    {
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
            future.get(ConnectionUtils.JMX_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e)
        {
            LOG.error("Jolokia connection failed with timeout or execution error", e);
            ConnectionUtils.closeQuietly(jmxConnector);
            throw new IOException("Jolokia connection failed", e);
        }
        finally
        {
            exec.shutdownNow();
        }
        verifyMBeanServerConnection(jmxConnector);
        return jmxConnector;
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
        if (String.valueOf(true).equals(tls.get(ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY)))
        {
            setJolokiaSslProperties();
        }
        return env;
    }

    private void setJolokiaSslProperties()
    {
        LOG.info("Setting Jolokia client with PEM certificates");
        Map<String, String> tls = myConnectionUtils.getTLSConfig();
        String caCert = tls.get(JOLOKIA_CA_CERTIFICATE_PROPERTY);
        String clientCert = tls.get(JOLOKIA_CLIENT_CERTIFICATE_PROPERTY);
        String clientKey = tls.get(JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY);
        String keyAlgorithm = tls.get(JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY);
        String disableHostnameVerification = tls.get(JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY);

        myConnectionUtils.setSystemPropertyIfNotNull(JOLOKIA_CA_CERTIFICATE_PROPERTY, caCert);
        myConnectionUtils.setSystemPropertyIfNotNull(JOLOKIA_CLIENT_CERTIFICATE_PROPERTY, clientCert);
        myConnectionUtils.setSystemPropertyIfNotNull(JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY, clientKey);
        myConnectionUtils.setSystemPropertyIfNotNull(JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY, keyAlgorithm);
        myConnectionUtils.setSystemPropertyIfNotNull(JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY, disableHostnameVerification);
    }

    /**
     * Verifies that the MBeanServerConnection is available.
     *
     * @param jmxConnector the JMX connector to verify.
     * @throws IOException if the connection is null.
     */
    public static void verifyMBeanServerConnection(final JMXConnector jmxConnector) throws IOException
    {
        if (jmxConnector.getMBeanServerConnection() == null)
        {
            ConnectionUtils.closeQuietly(jmxConnector);
            throw new IOException("MBeanServerConnection is null after Jolokia connection");
        }
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
     * Builder for constructing JolokiaConnectionUtils.
     */
    public static final class Builder
    {
        private ConnectionUtils myConnectionUtils;
        private int myPort = DEFAULT_JOLOKIA_PORT;

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
         * Build the JolokiaConnectionUtils instance.
         *
         * @return JolokiaConnectionUtils
         */
        public JolokiaConnectionStrategy build()
        {
            return new JolokiaConnectionStrategy(this);
        }
    }
}
