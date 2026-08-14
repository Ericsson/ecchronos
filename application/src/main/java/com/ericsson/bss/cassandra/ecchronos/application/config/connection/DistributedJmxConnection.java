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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.function.Supplier;

/**
 * Configuration class for distributed JMX connections.
 * Extends {@link Connection} to provide JMX-specific connection settings
 * including retry policies, Jolokia configuration, and DNS resolution options.
 */
public class DistributedJmxConnection extends Connection<DistributedJmxConnectionProvider>
{
    /** Default run delay in milliseconds. */
    public static final int DEFAULT_RUN_DELAY = 500;
    /** Default maximum wait time in minutes. */
    public static final int DEFAULT_MAX_WAIT_TIME_IN_MINUTES = 40;
    /** Default JMX port. */
    public static final int DEFAULT_JMX_PORT = 7199;
    private RetryPolicyConfig myRetryPolicyConfig = new RetryPolicyConfig();
    private JolokiaConfig myJolokiaConfig = new JolokiaConfig();
    private boolean myReverseDNSResolution = false;
    private Integer myRunDelay = DEFAULT_RUN_DELAY;
    private Integer myMaxWaitTimeInMinutes = DEFAULT_MAX_WAIT_TIME_IN_MINUTES;
    private boolean myUseBroadcastRPCAddress = true;
    private int myJmxPort = DEFAULT_JMX_PORT;

    /**
     * Default constructor. Sets the provider to {@link AgentJmxConnectionProvider}.
     */
    public DistributedJmxConnection()
    {
        try
        {
            setProvider(AgentJmxConnectionProvider.class);
        }
        catch (NoSuchMethodException ignored)
        {
            // Do something useful ...
        }
    }

    /**
     * Gets the JMX port.
     *
     * @return the JMX port.
     */
    @JsonProperty("port")
    public final int getJmxPort()
    {
        return myJmxPort;
    }

    /**
     * Sets the JMX port.
     *
     * @param port
     *         the JMX port to set.
     */
    @JsonProperty("port")
    public final void setJmxPort(final int port)
    {
        myJmxPort = port;
    }

    /**
     * Gets the run delay in milliseconds.
     *
     * @return the run delay.
     */
    @JsonProperty("runDelay")
    public final Integer getRunDelay()
    {
        return myRunDelay;
    }

    /**
     * Sets the run delay in milliseconds.
     *
     * @param runDelay
     *         the run delay to set.
     */
    @JsonProperty("runDelay")
    public final void setrunDelay(final Integer runDelay)
    {
        myRunDelay = runDelay;
    }

    /**
     * Gets the maximum wait time in minutes.
     *
     * @return the maximum wait time in minutes.
     */
    @JsonProperty("maxWaitTimeInMinutes")
    public final Integer getMaxWaitTimeInMinutes()
    {
        return myMaxWaitTimeInMinutes;
    }

    /**
     * Sets the maximum wait time in minutes.
     *
     * @param maxWaitTimeInMinutes
     *         the maximum wait time in minutes to set.
     */
    @JsonProperty("maxWaitTimeInMinutes")
    public final void setMaxWaitTimeInMinutes(final Integer maxWaitTimeInMinutes)
    {
        myMaxWaitTimeInMinutes = maxWaitTimeInMinutes;
    }

    /**
     * Gets the retry policy configuration.
     *
     * @return the retry policy configuration.
     */
    @JsonProperty("retryPolicy")
    public final RetryPolicyConfig getRetryPolicyConfig()
    {
        return myRetryPolicyConfig;
    }

    /**
     * Sets the retry policy configuration.
     *
     * @param retryPolicyConfig
     *         the retry policy configuration to set.
     */
    @JsonProperty("retryPolicy")
    public final void setRetryPolicyConfig(final RetryPolicyConfig retryPolicyConfig)
    {
        myRetryPolicyConfig = retryPolicyConfig;
    }

    /**
     * Gets the Jolokia configuration.
     *
     * @return the Jolokia configuration.
     */
    @JsonProperty("jolokia")
    public final JolokiaConfig getJolokiaConfig()
    {
        return myJolokiaConfig;
    }

    /**
     * Sets the Jolokia configuration.
     *
     * @param jolokiaConfig
     *         the Jolokia configuration to set.
     */
    @JsonProperty("jolokia")
    public final void setJolokiaConfig(final JolokiaConfig jolokiaConfig)
    {
        myJolokiaConfig = jolokiaConfig;
    }

    /**
     * Sets whether to use the broadcast RPC address.
     *
     * @param useBroadcastRPCAddress
     *         {@code true} to use the broadcast RPC address, {@code false} otherwise.
     */
    @JsonProperty("useBroadcastRPCAddress")
    public final void setUseBroadcastRPCAddress(final boolean useBroadcastRPCAddress)
    {
        myUseBroadcastRPCAddress = useBroadcastRPCAddress;
    }

    /**
     * Gets whether to use the broadcast RPC address.
     *
     * @return {@code true} if the broadcast RPC address is used, {@code false} otherwise.
     */
    @JsonProperty("useBroadcastRPCAddress")
    public final boolean getUseBroadcastRPCAddress()
    {
        return myUseBroadcastRPCAddress;
    }

    /**
     * Sets whether reverse DNS resolution is enabled.
     *
     * @param reverseDNSResolution
     *         {@code true} to enable reverse DNS resolution, {@code false} otherwise.
     */
    @JsonProperty("reverseDNSResolution")
    public final void setReseverseDNSResolution(final boolean reverseDNSResolution)
    {
        myReverseDNSResolution = reverseDNSResolution;
    }

    /**
     * Gets whether reverse DNS resolution is enabled.
     *
     * @return {@code true} if reverse DNS resolution is enabled, {@code false} otherwise.
     */
    @JsonProperty("reverseDNSResolution")
    public final boolean getReseverseDNSResolution()
    {
        return myReverseDNSResolution;
    }

    /**
     * {@inheritDoc}
     *
     * @return the expected constructor parameter types for the JMX connection provider.
     */
    @Override
    protected Class<?>[] expectedConstructor()
    {
        return new Class<?>[] {
                                Config.class,
                                Supplier.class,
                                DistributedNativeConnectionProvider.class,
                                EccNodesSync.class,
                                IpTranslator.class
        };
    }
}
