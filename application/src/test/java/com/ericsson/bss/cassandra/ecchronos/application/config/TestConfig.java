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
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Priority;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
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
    private static final TimeUnit TIME_UNIT_IN_SECONDS = TimeUnit.SECONDS;
    private static Config config;
    private static GlobalRepairConfig repairConfig;
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
        repairConfig = config.getRepairConfig();
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
        Interval connectionDelay = nativeConnection.getConnectionDelay();
        assertThat(connectionDelay.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(connectionDelay.getTime()).isEqualTo(45L);
    }

    @Test
    public void testRepairProvider()
    {
        Class<?> providerClass = repairConfig.getRepairConfigurationClass();
        assertEquals(FileBasedRepairConfiguration.class, providerClass);
    }

    @Test
    public void testRepairInterval()
    {
        Interval repairInterval = repairConfig.getRepairInterval();
        assertThat(repairInterval.getUnit()).isEqualTo(TimeUnit.HOURS);
        assertThat(repairInterval.getTime()).isEqualTo(1L);
    }

    @Test
    public void testRepairInitialDelay()
    {
        Interval repairInitialDelay = repairConfig.getInitialDelay();
        assertThat(repairInitialDelay.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(repairInitialDelay.getTime()).isEqualTo(1L);
    }

    @Test
    public void testRepairPriority()
    {
        Priority priority = repairConfig.getPriority();
        assertEquals(priority.getPriorityGranularityUnit(), TimeUnit.MINUTES);
    }

    @Test
    public void testRepairUnwindRatio()
    {
        Double repairUnwindRatio = repairConfig.asRepairConfiguration().getRepairUnwindRatio();
        assertEquals(repairUnwindRatio, Double.valueOf(0.5));
    }

    @Test
    public void testRepairHistoryLookback()
    {
        Interval repairHistoryLookback = repairConfig.getRepairHistoryLookback();
        assertThat(repairHistoryLookback.getUnit()).isEqualTo(TimeUnit.DAYS);
        assertThat(repairHistoryLookback.getTime()).isEqualTo(30L);
    }

    @Test
    public void testRepairSizeTarget()
    {
        long repairSizeTarget = repairConfig
                .asRepairConfiguration().getTargetRepairSizeInBytes();
        assertEquals(repairSizeTarget, 1L);
    }

    @Test
    public void testRepairIgnoreTWCSTables()
    {
        boolean repairIgnoreTWCSTables = repairConfig.getIgnoreTWCSTables();
        assertThat(repairIgnoreTWCSTables).isTrue();
    }

    @Test
    public void testRepairHistoryProvider()
    {
        RepairHistoryProvider repairHistoryProvider = repairConfig.getRepairHistory().getProvider();
        String keyspace = repairConfig.getRepairHistory().getKeyspaceName();
        assertEquals(repairHistoryProvider, RepairHistoryProvider.ECC);
        assertEquals(keyspace, "ecchronos");
    }

    @Test
    public void testRepairBackoff()
    {
        Interval repairBackoff = repairConfig.getBackoff();
        assertThat(repairBackoff.getUnit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(repairBackoff.getTime()).isEqualTo(35L);
    }

    @Test
    public void testRepairType()
    {
        RepairType repairType = repairConfig.getRepairType();
        assertThat(repairType).isEqualTo(RepairType.VNODE);
    }

    @Test
    public void testInstanceName()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getInstanceName()).isEqualTo("unique_identifier");
    }

    @Test
    public void testJolokiaConfig()
    {
        JolokiaConfig jolokiaConfig = config.getConnectionConfig().getJmxConnection().getJolokiaConfig();
        assertThat(jolokiaConfig).isNotNull();
        assertThat(jolokiaConfig.isEnabled()).isTrue();
        assertThat(jolokiaConfig.getPort()).isEqualTo(7887);
    }

    @Test
    public void testThreadPoolConfig()
    {
        ThreadPoolTaskConfig threadPoolTaskConfig = config.getConnectionConfig().getThreadPoolTaskConfig();
        assertNotNull(threadPoolTaskConfig);
        assertThat(threadPoolTaskConfig.getCorePoolSize()).isEqualTo(10);
        assertThat(threadPoolTaskConfig.getMaxPoolSize()).isEqualTo(50);
        assertThat(threadPoolTaskConfig.getQueueCapacity()).isEqualTo(10);
        assertThat(threadPoolTaskConfig.getKeepAliveSeconds()).isEqualTo(30);
    }

    @Test
    public void testCqlRetryPoliceConfig()
    {
        CQLRetryPolicyConfig cqlRetryPolicyConfig = config.getConnectionConfig().getCqlConnection().getCqlRetryPolicy();
        assertNotNull(cqlRetryPolicyConfig);
        assertThat(cqlRetryPolicyConfig.getMaxAttempts()).isEqualTo(10);
        assertThat(cqlRetryPolicyConfig.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(cqlRetryPolicyConfig.getDelay()).isEqualTo(600000);
        assertThat(cqlRetryPolicyConfig.getMaxDelay()).isEqualTo(2100000);
    }

    @Test
    public void testStatisticsConfig()
    {
        assertThat(config.getStatisticsConfig()).isNotNull();
        assertThat(config.getStatisticsConfig().isEnabled()).isFalse();
        assertThat(config.getStatisticsConfig().getOutputDirectory().getPath()).isEqualTo("./non-default-statistics");
        assertThat(config.getStatisticsConfig().getMetricsPrefix()).isEqualTo("unittest");
        assertThat(config.getStatisticsConfig().getRepairFailuresCount()).isEqualTo(5);
    }

    @Test
    public void testStatisticsRepairFailuresTimeWindow()
    {
        assertThat(config.getStatisticsConfig().getRepairFailuresTimeWindow().getTime()).isEqualTo(5L);
        assertThat(config.getStatisticsConfig().getRepairFailuresTimeWindow().getUnit()).isEqualTo(TimeUnit.MINUTES);
    }

    @Test
    public void testStatisticsTriggerInterval()
    {
        assertThat(config.getStatisticsConfig().getTriggerIntervalForMetricInspection().getTime()).isEqualTo(30L);
        assertThat(config.getStatisticsConfig().getTriggerIntervalForMetricInspection().getUnit()).isEqualTo(TimeUnit.SECONDS);
    }

    @Test
    public void testStatisticsReportingConfigs()
    {
        assertThat(config.getStatisticsConfig().getReportingConfigs()).isNotNull();
        assertThat(config.getStatisticsConfig().getReportingConfigs().getJmxReportingConfig().isEnabled()).isFalse();
        assertThat(config.getStatisticsConfig().getReportingConfigs().getFileReportingConfig().isEnabled()).isTrue();
        assertThat(config.getStatisticsConfig().getReportingConfigs().getHttpReportingConfig().isEnabled()).isTrue();
    }
}

