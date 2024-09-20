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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.*;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwarePolicy;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class TestConfig
{
    private static final String DEFAULT_AGENT_FILE_NAME = "all_set.yml";
    private static final String NOTHING_SET_AGENT_FILE_NAME = "nothing_set.yml";
    private static final TimeUnit TIME_UNIT_IN_SECONDS = TimeUnit.SECONDS;
    private static Config config;
    private static DistributedNativeConnection nativeConnection;
    private static DistributedJmxConnection distributedJmxConnection;

    @Before
    public void setup() throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        File file = new File(classLoader.getResource(DEFAULT_AGENT_FILE_NAME).getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        config = objectMapper.readValue(file, Config.class);

        ConnectionConfig connection = config.getConnectionConfig();

        nativeConnection = connection.getCqlConnection();
        distributedJmxConnection = connection.getJmxConnection();
    }

    @Test
    public void testDefaultAgentType()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getType()).isEqualTo(ConnectionType.datacenterAware);
    }

    @Test
    public void testLocalDatacenter()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getLocalDatacenter()).isEqualTo("datacenter1");
    }

    @Test
    public void testDefaultContactPoints()
    {
        assertThat(nativeConnection.getAgentConnectionConfig()).isNotNull();
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().get("127.0.0.1").getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().get("127.0.0.2").getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().size()).isEqualTo(2);
    }

    @Test
    public void testDefaultDatacenterAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getDatacenterAware()).isNotNull();
        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getDatacenterAware()
                .getDatacenters()
                .get("datacenter1")
                .getName()).isEqualTo("datacenter1");
    }

    @Test
    public void testDefaultRackAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getRackAware()).isNotNull();
        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getRackAware()
                .getRacks()
                .get("rack1")
                .getDatacenterName()).isEqualTo("datacenter1");
    }

    @Test
    public void testDefaultHostAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getHostAware()).isNotNull();
        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getHostAware()
                .getHosts()
                .get("127.0.0.1")
                .getPort())
                .isEqualTo(9042);

        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getHostAware()
                .getHosts()
                .get("127.0.0.2")
                .getPort())
                .isEqualTo(9042);

        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getHostAware()
                .getHosts()
                .get("127.0.0.3")
                .getPort())
                .isEqualTo(9042);

        assertThat(nativeConnection
                .getAgentConnectionConfig()
                .getHostAware()
                .getHosts()
                .get("127.0.0.4")
                .getPort())
                .isEqualTo(9042);
    }

    @Test
    public void testAgentProviderConfig()
    {
        Class<?> providerClass = nativeConnection.getProviderClass();
        assertThat(providerClass).isEqualTo(AgentNativeConnectionProvider.class);
    }

    @Test
    public void testConfigurationExceptionForWrongAgentType()
    {
        assertThrows(ConfigurationException.class, () ->
        {
            nativeConnection.getAgentConnectionConfig().setType("wrongType");
        });
    }

    @Test
    public void testRestServerConfig()
    {
        assertThat(config.getRestServer().getHost()).isEqualTo("127.0.0.2");
        assertThat(config.getRestServer().getPort()).isEqualTo(8081);
    }

    @Test
    public void testDefaultLoadBalancingPolicy()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getDatacenterAwarePolicy()).isEqualTo(DataCenterAwarePolicy.class);
    }

    @Test
    public void testRetryPolicyConfig()
    {
        Class<?> providerClass = distributedJmxConnection.getProviderClass();
        assertThat(providerClass).isEqualTo(AgentJmxConnectionProvider.class);
        RetryPolicyConfig retryPolicyConfig = distributedJmxConnection.getRetryPolicyConfig();
        assertNotNull(retryPolicyConfig);
        assertThat(5).isEqualTo(retryPolicyConfig.getMaxAttempts());
        assertThat(5000).isEqualTo(retryPolicyConfig.getRetryDelay().getStartDelay());
        assertThat(30000).isEqualTo(retryPolicyConfig.getRetryDelay().getMaxDelay());
        assertThat(TIME_UNIT_IN_SECONDS).isEqualTo(retryPolicyConfig.getRetryDelay().getUnit());
        assertThat(86400000).isEqualTo(retryPolicyConfig.getRetrySchedule().getInitialDelay());
        assertThat(86400000).isEqualTo(retryPolicyConfig.getRetrySchedule().getFixedDelay());
        assertThat(TIME_UNIT_IN_SECONDS).isEqualTo(retryPolicyConfig.getRetrySchedule().getUnit());
    }

    @Test
    public void testRetryPolicyConfigWhenNothingSet() throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(NOTHING_SET_AGENT_FILE_NAME).getFile());
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        Config config = objectMapper.readValue(file, Config.class);

        ConnectionConfig connection = config.getConnectionConfig();
        distributedJmxConnection = connection.getJmxConnection();
        Class<?> providerClass = distributedJmxConnection.getProviderClass();
        assertThat(providerClass).isEqualTo(AgentJmxConnectionProvider.class);
        RetryPolicyConfig retryPolicyConfig = distributedJmxConnection.getRetryPolicyConfig();
        assertNotNull(retryPolicyConfig);
        assertThat(5).isEqualTo(retryPolicyConfig.getMaxAttempts());
        assertThat(5000).isEqualTo(retryPolicyConfig.getRetryDelay().getStartDelay());
        assertThat(30000).isEqualTo(retryPolicyConfig.getRetryDelay().getMaxDelay());
        assertThat(TIME_UNIT_IN_SECONDS).isEqualTo(retryPolicyConfig.getRetryDelay().getUnit());
        assertThat(86400000).isEqualTo(retryPolicyConfig.getRetrySchedule().getInitialDelay());
        assertThat(86400000).isEqualTo(retryPolicyConfig.getRetrySchedule().getFixedDelay());
        assertThat(TIME_UNIT_IN_SECONDS).isEqualTo(retryPolicyConfig.getRetrySchedule().getUnit());
    }

    @Test
    public void testStartDelayGreaterThanMaxDelayThrowsException()
    {
        RetryPolicyConfig.RetryDelay retryDelay = new RetryPolicyConfig.RetryDelay();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
        {
            retryDelay.setMaxDelay(1000L);
            retryDelay.setStartDelay(2000L);
        });
        assertEquals("Start delay cannot be greater than max delay.", exception.getMessage());
    }

    @Test
    public void testMaxDelayLessThanStartDelayThrowsException()
    {
        RetryPolicyConfig.RetryDelay retryDelay = new RetryPolicyConfig.RetryDelay();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
        {
            retryDelay.setMaxDelay(3000L);
            retryDelay.setStartDelay(2000L);
            retryDelay.setMaxDelay(1000L);
        });
        assertEquals("Max delay cannot be less than start delay.", exception.getMessage());
    }

    @Test
    public void testConnectionDelay()
    {
        Interval connectionDelay = config.getConnectionConfig().getConnectionDelay();
        assertThat(connectionDelay.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(connectionDelay.getTime()).isEqualTo(45l);
    }
}

