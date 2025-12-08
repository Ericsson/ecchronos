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
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDefaultConfig
{
    private static final String DEFAULT_AGENT_FILE_NAME = "nothing_set.yml";
    private static final TimeUnit TIME_UNIT_IN_SECONDS = TimeUnit.SECONDS;
    private static Config config;
    private static GlobalRepairConfig repairConfig;

    @Before
    public void setup() throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        File file = new File(classLoader.getResource(DEFAULT_AGENT_FILE_NAME).getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        config = objectMapper.readValue(file, Config.class);
        repairConfig = config.getRepairConfig();

    }

    @Test
    public void testConnectionDelayDefault()
    {
        Interval connectionDelay = config.getConnectionConfig().getCqlConnection().getConnectionDelay();
        assertThat(connectionDelay.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(connectionDelay.getTime()).isEqualTo(60l);
    }

    @Test
    public void testTimeoutConfig()
    {
        DistributedNativeConnection.Timeout timeoutConfig = config.getConnectionConfig().getCqlConnection().getTimeout();
        assertNotNull(timeoutConfig);
        assertThat(timeoutConfig.getConnectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(0);
    }

    @Test
    public void testCqlRetryPoliceConfig()
    {
        CQLRetryPolicyConfig cqlRetryPolicyConfig = config.getConnectionConfig().getCqlConnection().getCqlRetryPolicy();
        assertNotNull(cqlRetryPolicyConfig);
        assertThat(cqlRetryPolicyConfig.getMaxAttempts()).isEqualTo(5);
        assertThat(cqlRetryPolicyConfig.getUnit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(cqlRetryPolicyConfig.getDelay()).isEqualTo(5000);
        assertThat(cqlRetryPolicyConfig.getMaxDelay()).isEqualTo(30000);
    }

    @Test
    public void testRetryPolicyConfigWhenNothingSet()
    {
        ConnectionConfig connection = config.getConnectionConfig();
        DistributedJmxConnection distributedJmxConnection = connection.getJmxConnection();
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
    public void testRepairProvider()
    {
        Class<?> providerClass = repairConfig.getRepairConfigurationClass();
        assertEquals(FileBasedRepairConfiguration.class, providerClass);
    }

    @Test
    public void testRepairInterval()
    {
        Interval repairInterval = repairConfig.getRepairInterval();
        assertThat(repairInterval.getUnit()).isEqualTo(TimeUnit.DAYS);
        assertThat(repairInterval.getTime()).isEqualTo(7L);
    }

    @Test
    public void testRepairInitialDelay()
    {
        Interval repairInitialDelay = repairConfig.getInitialDelay();
        assertThat(repairInitialDelay.getUnit()).isEqualTo(TimeUnit.DAYS);
        assertThat(repairInitialDelay.getTime()).isEqualTo(1L);
    }

    @Test
    public void testRepairPriority()
    {
        Priority priority = repairConfig.getPriority();
        assertEquals(priority.getPriorityGranularityUnit(), TimeUnit.HOURS);
    }

    @Test
    public void testRepairUnwindRatio()
    {
        Double repairUnwindRatio = repairConfig.asRepairConfiguration().getRepairUnwindRatio();
        assertEquals(repairUnwindRatio, Double.valueOf(0.0));
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
        assertEquals(repairSizeTarget, Long.MAX_VALUE);
    }

    @Test
    public void testRepairIgnoreTWCSTables()
    {
        boolean repairIgnoreTWCSTables = repairConfig.getIgnoreTWCSTables();
        assertThat(repairIgnoreTWCSTables).isFalse();
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
        assertThat(repairBackoff.getUnit()).isEqualTo(TimeUnit.MINUTES);
        assertThat(repairBackoff.getTime()).isEqualTo(30L);
    }

    @Test
    public void testRepairType()
    {
        RepairType repairType = repairConfig.getRepairType();
        assertThat(repairType).isEqualTo(RepairType.VNODE);
    }

    @Test
    public void testJolokiaConfig()
    {
        JolokiaConfig jolokiaConfig = config.getConnectionConfig().getJmxConnection().getJolokiaConfig();
        assertThat(jolokiaConfig).isNotNull();
        assertThat(jolokiaConfig.isEnabled()).isFalse();
        assertThat(jolokiaConfig.getPort()).isEqualTo(8778);
    }

    @Test
    public void testDefaultThreadPoolConfig()
    {
        ThreadPoolTaskConfig threadPoolTaskConfig = config.getConnectionConfig().getThreadPoolTaskConfig();
        assertNotNull(threadPoolTaskConfig);
        assertThat(threadPoolTaskConfig.getCorePoolSize()).isEqualTo(4);
        assertThat(threadPoolTaskConfig.getMaxPoolSize()).isEqualTo(10);
        assertThat(threadPoolTaskConfig.getQueueCapacity()).isEqualTo(20);
        assertThat(threadPoolTaskConfig.getKeepAliveSeconds()).isEqualTo(60);
    }

    @Test
    public void testPEMConfig()
    {
        assertThat(config.getConnectionConfig().getJmxConnection().getJolokiaConfig().usePem()).isFalse();
    }

    @Test
    public void testReverseDNSConfig()
    {
        assertThat(config.getConnectionConfig().getJmxConnection().getReseverseDNSResolution()).isFalse();
    }
}
