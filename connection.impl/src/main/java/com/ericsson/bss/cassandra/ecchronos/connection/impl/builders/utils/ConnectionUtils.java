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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.management.remote.JMXConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;

public final class ConnectionUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionUtils.class);
    public static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP
    public static final Integer JMX_CONNECTION_TIMEOUT = 20;

    private final boolean myReverseDNSResolution;
    private final IpTranslator myIpTranslator;
    private final Supplier<String[]> myCredentialsSupplier;
    private final Supplier<Map<String, String>> myTLSSupplier;

    public ConnectionUtils(final Builder builder)
    {
        myReverseDNSResolution = builder.myReverseDNSResolution;
        myIpTranslator = builder.myIpTranslator;
        myCredentialsSupplier = builder.myCredentialsSupplier;
        myTLSSupplier = builder.myTLSSupplier;
    }

    /**
     * Defines the host address for the given node.
     *
     * @param node the node to resolve.
     * @return the resolved host string.
     * @throws UnknownHostException if the host cannot be resolved.
     */
    public String defineHost(final Node node) throws UnknownHostException
    {
        String host = node.getBroadcastRpcAddress().get().getAddress().getHostAddress();
        if (NO_BROADCAST_ADDRESS.equals(host))
        {
            host = node.getListenAddress().get().getHostString();
        }
        if (myIpTranslator.isActive())
        {
            host = myIpTranslator.getInternalIp(host);
        }
        if (myReverseDNSResolution)
        {
            host = ReverseDNS.fromHostString(host);
        }
        if (host.contains(":") && !host.startsWith("[") && !host.endsWith("]"))
        {
            // Use square brackets to surround IPv6 addresses
            host = "[" + host + "]";
        }
        return host;
    }

    public static boolean isConnected(final JMXConnector jmxConnector)
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

    public static void closeQuietly(final JMXConnector connector)
    {
        if (connector != null)
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

    /**
     * Sets a system property if the value is not null, otherwise clears it.
     *
     * @param key the property key.
     * @param value the property value.
     */
    public void setSystemPropertyIfNotNull(final String key, final String value)
    {
        if (value != null)
        {
            System.setProperty(key, value);
        }
        else
        {
            System.clearProperty(key);
        }
    }

    /**
     * Get the credentials configuration.
     *
     * @return credentials array or null.
     */
    public String[] getCredentialsConfig()
    {
        if (myCredentialsSupplier == null)
        {
            return null;
        }
        return myCredentialsSupplier.get();
    }

    /**
     * Get the TLS configuration.
     *
     * @return TLS configuration map.
     */
    public Map<String, String> getTLSConfig()
    {
        if (myTLSSupplier == null)
        {
            return new HashMap<>();
        }
        return myTLSSupplier.get();
    }

    /**
     * Check if authentication is enabled.
     *
     * @return true if auth is enabled.
     */
    public boolean isAuthEnabled()
    {
        return getCredentialsConfig() != null;
    }

    /**
     * Check if TLS is enabled.
     *
     * @return true if TLS is enabled.
     */
    public boolean isTLSEnabled()
    {
        return !getTLSConfig().isEmpty();
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
     * Builder for constructing ConnectionUtils.
     */
    public static final class Builder
    {
        private Supplier<String[]> myCredentialsSupplier;
        private Supplier<Map<String, String>> myTLSSupplier;
        private boolean myReverseDNSResolution = false;
        private IpTranslator myIpTranslator;

        /**
         * Set credentials supplier.
         *
         * @param credentials the credentials supplier.
         * @return Builder
         */
        public Builder withCredentials(final Supplier<String[]> credentials)
        {
            myCredentialsSupplier = credentials;
            return this;
        }

        /**
         * Set TLS supplier.
         *
         * @param tls the TLS supplier.
         * @return Builder
         */
        public Builder withTls(final Supplier<Map<String, String>> tls)
        {
            myTLSSupplier = tls;
            return this;
        }

        /**
         * Set reverse DNS resolution.
         *
         * @param reverseDNSResolution whether to use reverse DNS.
         * @return Builder
         */
        public Builder withReverseDNSResolution(final boolean reverseDNSResolution)
        {
            myReverseDNSResolution = reverseDNSResolution;
            return this;
        }

        /**
         * Set IP translator.
         *
         * @param ipTranslator the IP translator.
         * @return Builder
         */
        public Builder withIpTranslator(final IpTranslator ipTranslator)
        {
            myIpTranslator = ipTranslator;
            return this;
        }

        /**
         * Build the ConnectionUtils instance.
         *
         * @return ConnectionUtils
         */
        public ConnectionUtils build()
        {
            return new ConnectionUtils(this);
        }
    }
}
