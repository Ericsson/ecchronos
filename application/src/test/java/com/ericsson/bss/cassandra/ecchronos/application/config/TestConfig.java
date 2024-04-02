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
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.Connection;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NativeConnection;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.LockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.ExcludedMetric;
import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.ReportingConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.StatisticsConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.application.config.rest.RestServerConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.runpolicy.RunPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.scheduler.SchedulerConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.CqlTLSConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import javax.management.remote.JMXConnector;
import javax.net.ssl.SSLEngine;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestConfig
{
    @Test
    public void testAllValues() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("all_set.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        ConnectionConfig connection = config.getConnectionConfig();

        NativeConnection nativeConnection = connection.getCqlConnection();
        assertThat(nativeConnection.getHost()).isEqualTo("127.0.0.2");
        assertThat(nativeConnection.getPort()).isEqualTo(9100);
        assertThat(nativeConnection.getRemoteRouting()).isFalse();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.SECONDS)).isEqualTo(5);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(TestNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(TestCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(TestStatementDecorator.class);

        Connection jmxConnection = connection.getJmxConnection();
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
                .withPriorityGranularityUnit(TimeUnit.MINUTES)
                .build();

        GlobalRepairConfig repairConfig = config.getRepairConfig();
        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getRepairLockType()).isEqualTo(RepairLockType.DATACENTER);
        assertThat(repairConfig.getRepairConfigurationClass()).isEqualTo(TestRepairConfigurationProvider.class);

        assertThat(repairConfig.getRepairHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(13);
        assertThat(repairConfig.getRepairHistory().getProvider()).isEqualTo(RepairHistory.Provider.CASSANDRA);
        assertThat(repairConfig.getRepairHistory().getKeyspaceName()).isEqualTo("customkeyspace");
        assertThat(repairConfig.getAlarm().getFaultReporterClass()).isEqualTo(TestFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isTrue();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.SECONDS)).isEqualTo(13);
        assertThat(repairConfig.getPriority().getPriorityGranularityUnit()).isEqualTo(TimeUnit.MINUTES);

        StatisticsConfig statisticsConfig = config.getStatisticsConfig();
        assertThat(statisticsConfig.isEnabled()).isFalse();
        assertThat(statisticsConfig.getOutputDirectory()).isEqualTo(new File("./non-default-statistics"));
        assertThat(statisticsConfig.getMetricsPrefix()).isEqualTo("unittest");

        assertThat(statisticsConfig.getReportingConfigs().isJmxReportingEnabled()).isFalse();
        ReportingConfig jmxReportingConfig = statisticsConfig.getReportingConfigs().getJmxReportingConfig();
        assertThat(jmxReportingConfig.isEnabled()).isFalse();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isFileReportingEnabled()).isTrue();
        ReportingConfig fileReportingConfig = statisticsConfig.getReportingConfigs().getFileReportingConfig();
        ExcludedMetric expectedFileExcludedMetric = new ExcludedMetric();
        expectedFileExcludedMetric.setMetricName(".*fileExcluded");
        Map<String, String> excludedFileTags = new HashMap<>();
        excludedFileTags.put("keyspace", "filekeyspace");
        excludedFileTags.put("table", ".*table");
        expectedFileExcludedMetric.setMetricTags(excludedFileTags);
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).hasSize(1);
        assertThat(fileReportingConfig.getExcludedMetrics()).contains(expectedFileExcludedMetric);

        assertThat(statisticsConfig.getReportingConfigs().isHttpReportingEnabled()).isTrue();
        ReportingConfig httpReportingConfig = statisticsConfig.getReportingConfigs().getHttpReportingConfig();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        ExcludedMetric expectedHttpExcludedMetric = new ExcludedMetric();
        expectedHttpExcludedMetric.setMetricName(".*httpExcluded");
        assertThat(httpReportingConfig.getExcludedMetrics()).hasSize(1);
        assertThat(httpReportingConfig.getExcludedMetrics()).contains(expectedHttpExcludedMetric);

        LockFactoryConfig lockFactoryConfig = config.getLockFactory();
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getKeyspaceName()).isEqualTo("ecc");
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getConsistencySerial().equals(ConsistencyType.LOCAL)).isTrue();

        RunPolicyConfig runPolicyConfig = config.getRunPolicy();
        assertThat(runPolicyConfig.getTimeBasedConfig().getKeyspaceName()).isEqualTo("ecc");

        SchedulerConfig schedulerConfig = config.getSchedulerConfig();
        assertThat(schedulerConfig.getFrequency().getInterval(TimeUnit.SECONDS)).isEqualTo(60);

        RestServerConfig restServerConfig = config.getRestServer();
        assertThat(restServerConfig.getHost()).isEqualTo("127.0.0.2");
        assertThat(restServerConfig.getPort()).isEqualTo(8081);
    }
    @Test
    public void testIssue264() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("issue264.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);
        GlobalRepairConfig repair = config.getRepairConfig();
        Interval interval = repair.getInitialDelay();
        long x = interval.getInterval(TimeUnit.HOURS);
        assertThat(interval.getInterval(TimeUnit.HOURS)).isEqualTo(5);

    }

    @Test
    public void testIssue264_faultyConfig() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("issue264_faulty.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        assertThatExceptionOfType(JsonMappingException.class).isThrownBy(() -> objectMapper.readValue(file, Config.class));

    }

    @Test
    public void testWithDefaultFile() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("ecc.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        ConnectionConfig connection = config.getConnectionConfig();

        NativeConnection nativeConnection = connection.getCqlConnection();
        assertThat(nativeConnection.getHost()).isEqualTo("localhost");
        assertThat(nativeConnection.getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getRemoteRouting()).isTrue();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(0);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(ReloadingCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(NoopStatementDecorator.class);

        Connection jmxConnection = connection.getJmxConnection();
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

        GlobalRepairConfig repairConfig = config.getRepairConfig();

        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getRepairLockType()).isEqualTo(RepairLockType.VNODE);
        assertThat(repairConfig.getRepairConfigurationClass()).isEqualTo(FileBasedRepairConfiguration.class);

        assertThat(repairConfig.getRepairHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(30);
        assertThat(repairConfig.getRepairHistory().getProvider()).isEqualTo(RepairHistory.Provider.ECC);
        assertThat(repairConfig.getRepairHistory().getKeyspaceName()).isEqualTo("ecchronos");
        assertThat(repairConfig.getAlarm().getFaultReporterClass()).isEqualTo(LoggingFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isFalse();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.MINUTES)).isEqualTo(30);
        assertThat(repairConfig.getPriority().getPriorityGranularityUnit()).isEqualTo(TimeUnit.HOURS);

        assertThat(repairConfig.getInitialDelay().getInterval(TimeUnit.HOURS)).isEqualTo(1);

        StatisticsConfig statisticsConfig = config.getStatisticsConfig();
        assertThat(statisticsConfig.isEnabled()).isTrue();
        assertThat(statisticsConfig.getOutputDirectory()).isEqualTo(new File("./statistics"));
        assertThat(statisticsConfig.getMetricsPrefix()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isJmxReportingEnabled()).isTrue();
        ReportingConfig jmxReportingConfig = statisticsConfig.getReportingConfigs().getJmxReportingConfig();
        assertThat(jmxReportingConfig.isEnabled()).isTrue();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isFileReportingEnabled()).isTrue();
        ReportingConfig fileReportingConfig = statisticsConfig.getReportingConfigs().getFileReportingConfig();
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isHttpReportingEnabled()).isTrue();
        ReportingConfig httpReportingConfig = statisticsConfig.getReportingConfigs().getHttpReportingConfig();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        assertThat(httpReportingConfig.getExcludedMetrics()).isEmpty();

        LockFactoryConfig lockFactoryConfig = config.getLockFactory();
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getKeyspaceName()).isEqualTo("ecchronos");
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getConsistencySerial().equals(ConsistencyType.DEFAULT)).isTrue();

        RunPolicyConfig runPolicyConfig = config.getRunPolicy();
        assertThat(runPolicyConfig.getTimeBasedConfig().getKeyspaceName()).isEqualTo("ecchronos");

        SchedulerConfig schedulerConfig = config.getSchedulerConfig();
        assertThat(schedulerConfig.getFrequency().getInterval(TimeUnit.SECONDS)).isEqualTo(30);

        RestServerConfig restServerConfig = config.getRestServer();
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

        ConnectionConfig connection = config.getConnectionConfig();

        NativeConnection nativeConnection = connection.getCqlConnection();
        assertThat(nativeConnection.getHost()).isEqualTo("localhost");
        assertThat(nativeConnection.getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getRemoteRouting()).isTrue();
        assertThat(nativeConnection.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS)).isEqualTo(0);
        assertThat(nativeConnection.getProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(nativeConnection.getCertificateHandlerClass()).isEqualTo(ReloadingCertificateHandler.class);
        assertThat(nativeConnection.getDecoratorClass()).isEqualTo(NoopStatementDecorator.class);

        Connection jmxConnection = connection.getJmxConnection();
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

        GlobalRepairConfig repairConfig = config.getRepairConfig();

        assertThat(repairConfig.asRepairConfiguration()).isEqualTo(expectedConfiguration);

        assertThat(repairConfig.getRepairLockType()).isEqualTo(RepairLockType.VNODE);
        assertThat(repairConfig.getRepairConfigurationClass()).isEqualTo(FileBasedRepairConfiguration.class);

        assertThat(repairConfig.getRepairHistoryLookback().getInterval(TimeUnit.DAYS)).isEqualTo(30);
        assertThat(repairConfig.getRepairHistory().getProvider()).isEqualTo(RepairHistory.Provider.ECC);
        assertThat(repairConfig.getRepairHistory().getKeyspaceName()).isEqualTo("ecchronos");
        assertThat(repairConfig.getAlarm().getFaultReporterClass()).isEqualTo(LoggingFaultReporter.class);
        assertThat(repairConfig.getIgnoreTWCSTables()).isFalse();
        assertThat(repairConfig.getBackoff().getInterval(TimeUnit.MINUTES)).isEqualTo(30);


        StatisticsConfig statisticsConfig = config.getStatisticsConfig();
        assertThat(statisticsConfig.isEnabled()).isTrue();
        assertThat(statisticsConfig.getOutputDirectory()).isEqualTo(new File("./statistics"));
        assertThat(statisticsConfig.getMetricsPrefix()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isJmxReportingEnabled()).isTrue();
        ReportingConfig jmxReportingConfig = statisticsConfig.getReportingConfigs().getJmxReportingConfig();
        assertThat(jmxReportingConfig.isEnabled()).isTrue();
        assertThat(jmxReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isFileReportingEnabled()).isTrue();
        ReportingConfig fileReportingConfig = statisticsConfig.getReportingConfigs().getFileReportingConfig();
        assertThat(fileReportingConfig.isEnabled()).isTrue();
        assertThat(fileReportingConfig.getExcludedMetrics()).isEmpty();

        assertThat(statisticsConfig.getReportingConfigs().isHttpReportingEnabled()).isTrue();
        ReportingConfig httpReportingConfig = statisticsConfig.getReportingConfigs().getHttpReportingConfig();
        assertThat(httpReportingConfig.isEnabled()).isTrue();
        assertThat(httpReportingConfig.getExcludedMetrics()).isEmpty();

        LockFactoryConfig lockFactoryConfig = config.getLockFactory();
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getKeyspaceName()).isEqualTo("ecchronos");
        assertThat(lockFactoryConfig.getCasLockFactoryConfig().getConsistencySerial().equals(ConsistencyType.DEFAULT)).isTrue();

        RunPolicyConfig runPolicyConfig = config.getRunPolicy();
        assertThat(runPolicyConfig.getTimeBasedConfig().getKeyspaceName()).isEqualTo("ecchronos");

        SchedulerConfig schedulerConfig = config.getSchedulerConfig();
        assertThat(schedulerConfig.getFrequency().getInterval(TimeUnit.SECONDS)).isEqualTo(30);

        RestServerConfig restServerConfig = config.getRestServer();
        assertThat(restServerConfig.getHost()).isEqualTo("localhost");
        assertThat(restServerConfig.getPort()).isEqualTo(8080);
    }

    @Test
    public void testRepairIntervalLongerThanWarn()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair_interval_longer_than_warn.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(JsonMappingException.class).isThrownBy(() -> objectMapper.readValue(file, Config.class));
    }

    @Test
    public void testWarnIntervalLongerThanError()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("warn_interval_longer_than_error.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(JsonMappingException.class).isThrownBy(() -> objectMapper.readValue(file, Config.class));
    }

    @Test
    public void testStatisticsDisabledIfNoReporting() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("all_reporting_disabled.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Config config = objectMapper.readValue(file, Config.class);

        StatisticsConfig statisticsConfig = config.getStatisticsConfig();
        assertThat(statisticsConfig.getReportingConfigs().isJmxReportingEnabled()).isFalse();
        assertThat(statisticsConfig.getReportingConfigs().isFileReportingEnabled()).isFalse();
        assertThat(statisticsConfig.getReportingConfigs().isHttpReportingEnabled()).isFalse();
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
        public TestCertificateHandler(Supplier<CqlTLSConfig> tlsConfigSupplier)
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
        public Set<RepairConfiguration> forTable(TableReference tableReference)
        {
            return Collections.emptySet();
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
