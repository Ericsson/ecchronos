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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.application.NoopStatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.application.ReloadingCertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import javax.management.remote.JMXConnector;
import javax.net.ssl.SSLEngine;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
        assertThat(nativeConnection.getRemoteRouting()).isFalse();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.SECONDS)).isEqualTo(5);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(TestNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(TestCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(TestStatementDecorator.class);

        Config.Connection jmxConnection = connection.getJmx();
        assertThat(jmxConnection.getHost()).isEqualTo("127.0.0.3");
        assertThat(jmxConnection.getPort()).isEqualTo(7100);
        assertThat(jmxConnection.getProviderClass()).isEqualTo(TestJmxConnectionProvider.class);

        RepairConfiguration expectedConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(24, TimeUnit.HOURS)
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(48, TimeUnit.HOURS)
                .withRepairErrorTime(72, TimeUnit.HOURS)
                .withRepairUnwindRatio(0.5d)
                .withIgnoreTWCSTables(true)
                .withBackoff(13, TimeUnit.SECONDS)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("5m"))
                .build();

        Config.GlobalRepairConfig repairConfig = config.getRepair();
        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getLockType()).isEqualTo(RepairLockType.DATACENTER);
        assertThat(repairConfig.getProvider()).isEqualTo(TestRepairConfigurationProvider.class);

        assertThat(repairConfig.getHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(13);
        assertThat(repairConfig.getHistory().getProvider()).isEqualTo(Config.RepairHistory.Provider.CASSANDRA);
        assertThat(repairConfig.getHistory().getKeyspace()).isEqualTo("customkeyspace");
        assertThat(repairConfig.getAlarm().getFaultReporter()).isEqualTo(TestFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isTrue();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.SECONDS)).isEqualTo(13);

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.isEnabled()).isFalse();
        assertThat(statisticsConfig.getDirectory()).isEqualTo(new File("./non-default-statistics"));
        assertThat(statisticsConfig.getPrefix()).isEqualTo("unittest");

        assertThat(statisticsConfig.getReporting().isJmxReportingEnabled()).isFalse();
        Config.ReportingConfig jmxReportingConfig = statisticsConfig.getReporting().getJmx();
        assertThat(jmxReportingConfig.isEnabled()).isFalse();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReporting().isFileReportingEnabled()).isTrue();
        Config.ReportingConfig fileReportingConfig = statisticsConfig.getReporting().getFile();
        Config.ExcludedMetric expectedFileExcludedMetric = new Config.ExcludedMetric();
        expectedFileExcludedMetric.setName(".*fileExcluded");
        Map<String, String> excludedFileTags = new HashMap<>();
        excludedFileTags.put("keyspace", "filekeyspace");
        excludedFileTags.put("table", ".*table");
        expectedFileExcludedMetric.setTags(excludedFileTags);
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).hasSize(1);
        assertThat(fileReportingConfig.getExcludedMetrics()).contains(expectedFileExcludedMetric);

        assertThat(statisticsConfig.getReporting().isHttpReportingEnabled()).isTrue();
        Config.ReportingConfig httpReportingConfig = statisticsConfig.getReporting().getHttp();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        Config.ExcludedMetric expectedHttpExcludedMetric = new Config.ExcludedMetric();
        expectedHttpExcludedMetric.setName(".*httpExcluded");
        assertThat(httpReportingConfig.getExcludedMetrics()).hasSize(1);
        assertThat(httpReportingConfig.getExcludedMetrics()).contains(expectedHttpExcludedMetric);

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
    public void testWithDefaultFile() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("ecc.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        Config.ConnectionConfig connection = config.getConnectionConfig();

        Config.NativeConnection nativeConnection = connection.getCql();
        assertThat(nativeConnection.getHost()).isEqualTo("localhost");
        assertThat(nativeConnection.getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getRemoteRouting()).isTrue();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(0);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(ReloadingCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(NoopStatementDecorator.class);

        Config.Connection jmxConnection = connection.getJmx();
        assertThat(jmxConnection.getHost()).isEqualTo("localhost");
        assertThat(jmxConnection.getPort()).isEqualTo(7199);
        assertThat(jmxConnection.getProviderClass()).isEqualTo(DefaultJmxConnectionProvider.class);

        RepairConfiguration expectedConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(7, TimeUnit.DAYS)
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(8, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .withRepairUnwindRatio(0.0d)
                .withIgnoreTWCSTables(false)
                .withBackoff(30, TimeUnit.MINUTES)
                .withTargetRepairSizeInBytes(RepairConfiguration.FULL_REPAIR_SIZE)
                .build();

        Config.GlobalRepairConfig repairConfig = config.getRepair();

        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getLockType()).isEqualTo(RepairLockType.VNODE);
        assertThat(repairConfig.getProvider()).isEqualTo(FileBasedRepairConfiguration.class);

        assertThat(repairConfig.getHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(30);
        assertThat(repairConfig.getHistory().getProvider()).isEqualTo(Config.RepairHistory.Provider.ECC);
        assertThat(repairConfig.getHistory().getKeyspace()).isEqualTo("ecchronos");
        assertThat(repairConfig.getAlarm().getFaultReporter()).isEqualTo(LoggingFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isFalse();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.MINUTES)).isEqualTo(30);

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.isEnabled()).isTrue();
        assertThat(statisticsConfig.getDirectory()).isEqualTo(new File("./statistics"));
        assertThat(statisticsConfig.getPrefix()).isEmpty();

        assertThat(statisticsConfig.getReporting().isJmxReportingEnabled()).isTrue();
        Config.ReportingConfig jmxReportingConfig = statisticsConfig.getReporting().getJmx();
        assertThat(jmxReportingConfig.isEnabled()).isTrue();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReporting().isFileReportingEnabled()).isTrue();
        Config.ReportingConfig fileReportingConfig = statisticsConfig.getReporting().getFile();
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReporting().isHttpReportingEnabled()).isTrue();
        Config.ReportingConfig httpReportingConfig = statisticsConfig.getReporting().getHttp();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        assertThat(httpReportingConfig.getExcludedMetrics()).isEmpty();

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

    @Test
    public void testDefault() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("nothing_set.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        Config.ConnectionConfig connection = config.getConnectionConfig();

        Config.NativeConnection nativeConnection = connection.getCql();
        assertThat(nativeConnection.getHost()).isEqualTo("localhost");
        assertThat(nativeConnection.getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getRemoteRouting()).isTrue();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(0);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(ReloadingCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(NoopStatementDecorator.class);

        Config.Connection jmxConnection = connection.getJmx();
        assertThat(jmxConnection.getHost()).isEqualTo("localhost");
        assertThat(jmxConnection.getPort()).isEqualTo(7199);
        assertThat(jmxConnection.getProviderClass()).isEqualTo(DefaultJmxConnectionProvider.class);

        RepairConfiguration expectedConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(7, TimeUnit.DAYS)
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(8, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .withRepairUnwindRatio(0.0d)
                .withBackoff(30, TimeUnit.MINUTES)
                .withTargetRepairSizeInBytes(RepairConfiguration.FULL_REPAIR_SIZE)
                .build();

        Config.GlobalRepairConfig repairConfig = config.getRepair();

        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getLockType()).isEqualTo(RepairLockType.VNODE);
        assertThat(repairConfig.getProvider()).isEqualTo(FileBasedRepairConfiguration.class);

        assertThat(repairConfig.getHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(30);
        assertThat(repairConfig.getHistory().getProvider()).isEqualTo(Config.RepairHistory.Provider.ECC);
        assertThat(repairConfig.getHistory().getKeyspace()).isEqualTo("ecchronos");
        assertThat(repairConfig.getAlarm().getFaultReporter()).isEqualTo(LoggingFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isFalse();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.MINUTES)).isEqualTo(30);

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.isEnabled()).isTrue();
        assertThat(statisticsConfig.getDirectory()).isEqualTo(new File("./statistics"));
        assertThat(statisticsConfig.getPrefix()).isEmpty();

        assertThat(statisticsConfig.getReporting().isJmxReportingEnabled()).isTrue();
        Config.ReportingConfig jmxReportingConfig = statisticsConfig.getReporting().getJmx();
        assertThat(jmxReportingConfig.isEnabled()).isTrue();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReporting().isFileReportingEnabled()).isTrue();
        Config.ReportingConfig fileReportingConfig = statisticsConfig.getReporting().getFile();
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReporting().isHttpReportingEnabled()).isTrue();
        Config.ReportingConfig httpReportingConfig = statisticsConfig.getReporting().getHttp();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        assertThat(httpReportingConfig.getExcludedMetrics()).isEmpty();

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

    @Test
    public void testStatisticsDisabledIfNoReporting() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("all_reporting_disabled.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        Config.StatisticsConfig statisticsConfig = config.getStatistics();
        assertThat(statisticsConfig.getReporting().isJmxReportingEnabled()).isFalse();
        assertThat(statisticsConfig.getReporting().isFileReportingEnabled()).isFalse();
        assertThat(statisticsConfig.getReporting().isHttpReportingEnabled()).isFalse();
        assertThat(statisticsConfig.isEnabled()).isFalse();
    }

    public static class TestNativeConnectionProvider implements NativeConnectionProvider
    {
        public TestNativeConnectionProvider(Config config, Supplier<Security.CqlSecurity> cqlSecurity,
                DefaultRepairConfigurationProvider defaultRepairConfigurationProvider, MeterRegistry meterRegistry)
        {
            // Empty constructor
        }

        @Override
        public CqlSession getSession()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node getLocalNode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getRemoteRouting()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TestCertificateHandler implements CertificateHandler
    {
        public TestCertificateHandler(Supplier<TLSConfig> tlsConfigSupplier)
        {
            // Empty constructor
        }

        @Override
        public SSLEngine newSslEngine(EndPoint remoteEndpoint)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception
        {
            // Empty, nothing to close
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

    public static class TestRepairConfigurationProvider extends AbstractRepairConfigurationProvider
    {
        protected TestRepairConfigurationProvider(ApplicationContext applicationContext)
        {
            super(applicationContext);
        }

        @Override
        public Optional<RepairConfiguration> forTable(TableReference tableReference)
        {
            return Optional.empty();
        }
    }

    public static class TestFaultReporter implements RepairFaultReporter
    {

        @Override
        public void raise(FaultCode faultCode, Map<String, Object> data)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cease(FaultCode faultCode, Map<String, Object> data)
        {
            throw new UnsupportedOperationException();
        }
    }
}
