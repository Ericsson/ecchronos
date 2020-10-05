/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.NoopStatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import javax.management.remote.JMXConnector;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class TestConfig
{
    @Test
    public void testAllValues() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("all_set.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        Config.ConnectionConfig connection = config.getConnectionConfig();

        Config.NativeConnection nativeConnection = connection.getCql();
        assertThat(nativeConnection.getHost()).isEqualTo("127.0.0.2");
        assertThat(nativeConnection.getPort()).isEqualTo(9100);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(TestNativeConnectionProvider.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(TestStatementDecorator.class);

        Config.Connection jmxConnection = connection.getJmx();
        assertThat(jmxConnection.getHost()).isEqualTo("127.0.0.3");
        assertThat(jmxConnection.getPort()).isEqualTo(7100);
        assertThat(jmxConnection.getProviderClass()).isEqualTo(TestJmxConnectionProvider.class);

        Config.RepairConfig repairConfig = config.getRepair();

        Config.Interval repairInterval = repairConfig.getInterval();
        assertThat(repairInterval.getInterval(TimeUnit.HOURS)).isEqualTo(24);

        assertThat(repairConfig.getParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(repairConfig.getLockType()).isEqualTo(RepairLockType.DATACENTER);

        assertThat(repairConfig.getAlarm().getWarn().getInterval(TimeUnit.HOURS)).isEqualTo(48);
        assertThat(repairConfig.getAlarm().getError().getInterval(TimeUnit.HOURS)).isEqualTo(72);

        assertThat(repairConfig.getUnwindRatio()).isEqualTo(0.5d);
        assertThat(repairConfig.getHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(13);
        assertThat(repairConfig.getSizeTargetInBytes()).isEqualTo(UnitConverter.toBytes("5m"));

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.isEnabled()).isFalse();
        assertThat(statisticsConfig.getDirectory()).isEqualTo(new File("./non-default-statistics"));

        Config.LockFactoryConfig lockFactoryConfig = config.getLockFactory();
        assertThat(lockFactoryConfig.getCas().getKeyspace()).isEqualTo("ecc");

        Config.RunPolicyConfig runPolicyConfig = config.getRunPolicy();
        assertThat(runPolicyConfig.getTimeBased().getKeyspace()).isEqualTo("ecc");

        Config.SchedulerConfig schedulerConfig = config.getScheduler();
        assertThat(schedulerConfig.getFrequency().getInterval(TimeUnit.SECONDS)).isEqualTo(60);

        Config.RestServerConfig restServerConfig = config.getRestServer();
        assertThat(restServerConfig.getHost()).isEqualTo("127.0.0.2");
        assertThat(restServerConfig.getPort()).isEqualTo(8081);
    }

    @Test
    public void testDefault() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("default.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        Config.ConnectionConfig connection = config.getConnectionConfig();

        Config.NativeConnection nativeConnection = connection.getCql();
        assertThat(nativeConnection.getHost()).isEqualTo("localhost");
        assertThat(nativeConnection.getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(NoopStatementDecorator.class);

        Config.Connection jmxConnection = connection.getJmx();
        assertThat(jmxConnection.getHost()).isEqualTo("localhost");
        assertThat(jmxConnection.getPort()).isEqualTo(7199);
        assertThat(jmxConnection.getProviderClass()).isEqualTo(DefaultJmxConnectionProvider.class);

        Config.RepairConfig repairConfig = config.getRepair();

        Config.Interval repairInterval = repairConfig.getInterval();
        assertThat(repairInterval.getInterval(TimeUnit.DAYS)).isEqualTo(7);

        assertThat(repairConfig.getParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(repairConfig.getLockType()).isEqualTo(RepairLockType.VNODE);

        assertThat(repairConfig.getAlarm().getWarn().getInterval(TimeUnit.DAYS)).isEqualTo(8);
        assertThat(repairConfig.getAlarm().getError().getInterval(TimeUnit.DAYS)).isEqualTo(10);

        assertThat(repairConfig.getUnwindRatio()).isEqualTo(0.0d);
        assertThat(repairConfig.getHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(30);
        assertThat(repairConfig.getSizeTargetInBytes()).isEqualTo(Long.MAX_VALUE);

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.isEnabled()).isTrue();
        assertThat(statisticsConfig.getDirectory()).isEqualTo(new File("./statistics"));

        Config.LockFactoryConfig lockFactoryConfig = config.getLockFactory();
        assertThat(lockFactoryConfig.getCas().getKeyspace()).isEqualTo("ecchronos");

        Config.RunPolicyConfig runPolicyConfig = config.getRunPolicy();
        assertThat(runPolicyConfig.getTimeBased().getKeyspace()).isEqualTo("ecchronos");

        Config.SchedulerConfig schedulerConfig = config.getScheduler();
        assertThat(schedulerConfig.getFrequency().getInterval(TimeUnit.SECONDS)).isEqualTo(30);

        Config.RestServerConfig restServerConfig = config.getRestServer();
        assertThat(restServerConfig.getHost()).isEqualTo("localhost");
        assertThat(restServerConfig.getPort()).isEqualTo(8080);
    }

    public static class TestNativeConnectionProvider implements NativeConnectionProvider
    {
        public TestNativeConnectionProvider(Config config, Supplier<Security.CqlSecurity> cqlSecurity)
        {
            // Empty constructor
        }

        @Override
        public Session getSession()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Host getLocalHost()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestJmxConnectionProvider implements JmxConnectionProvider
    {
        public TestJmxConnectionProvider(Config config, Supplier<Security.JmxSecurity> jmxSecurity)
        {
            // Empty constructor
        }

        @Override
        public JMXConnector getJmxConnector()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestStatementDecorator implements StatementDecorator
    {
        public TestStatementDecorator(Config config)
        {
            // Empty constructor
        }

        @Override
        public Statement apply(Statement statement)
        {
            throw new UnsupportedOperationException();
        }
    }
}
