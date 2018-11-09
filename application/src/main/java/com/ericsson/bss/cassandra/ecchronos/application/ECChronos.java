/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.DefaultStatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;

public class ECChronos implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronos.class);

    private static final String CONFIGURATION_FILE_PATH = "ecchronos.config";
    private static final String DEFAULT_CONFIGURATION_FILE = "./ecChronos.cfg";

    private final Properties myConfiguration;

    private final ECChronosInternals myECChronosInternals;

    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final RepairSchedulerImpl myRepairSchedulerImpl;

    public ECChronos(Properties configuration, RepairFaultReporter repairFaultReporter,
                     NativeConnectionProvider nativeConnectionProvider,
                     JmxConnectionProvider jmxConnectionProvider) throws ConfigurationException
    {
        myConfiguration = configuration;

        StatementDecorator statementDecorator = new DefaultStatementDecorator();

        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider,jmxConnectionProvider, statementDecorator);

        Host host = nativeConnectionProvider.getLocalHost();
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();

        RepairHistoryProviderImpl repairHistoryProvider = new RepairHistoryProviderImpl(nativeConnectionProvider.getSession(), statementDecorator);

        RepairStateFactoryImpl repairStateFactoryImpl = RepairStateFactoryImpl.builder()
                .withMetadata(metadata)
                .withHost(host)
                .withHostStates(myECChronosInternals.getHostStates())
                .withRepairHistoryProvider(repairHistoryProvider)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .build();

        TimeBasedRunPolicyProperties timeBasedRunPolicyProperties = TimeBasedRunPolicyProperties.from(configuration);

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(nativeConnectionProvider.getSession())
                .withStatementDecorator(statementDecorator)
                .withKeyspaceName(timeBasedRunPolicyProperties.getKeyspaceName())
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withFaultReporter(repairFaultReporter)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withRepairStateFactory(repairStateFactoryImpl)
                .build();

        myDefaultRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withCluster(nativeConnectionProvider.getSession().getCluster())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withDefaultRepairConfiguration(getRepairConfiguration())
                .build();
    }

    public void start()
    {
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
    }

    @Override
    public void close()
    {
        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);

        myTimeBasedRunPolicy.close();
        myDefaultRepairConfigurationProvider.close();
        myRepairSchedulerImpl.close();

        myECChronosInternals.close();
    }

    private RepairConfiguration getRepairConfiguration() throws ConfigurationException
    {
        RepairProperties repairProperties = RepairProperties.from(myConfiguration);

        LOG.debug("Using repair properties {}", repairProperties);

        return RepairConfiguration.newBuilder()
                .withRepairInterval(repairProperties.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withType(repairProperties.getRepairType())
                .withParallelism(repairProperties.getRepairParallelism())
                .withRepairWarningTime(repairProperties.getRepairAlarmWarnInMs(), TimeUnit.MILLISECONDS)
                .withRepairErrorTime(repairProperties.getRepairAlarmErrorInMs(), TimeUnit.MILLISECONDS)
                .build();
    }

    public static void main(String[] args) throws IOException
    {
        boolean foreground = false;

        if (args.length >= 1 && "-f".equals(args[0])) // NOPMD
        {
            foreground = true;
        }

        LocalNativeConnectionProvider nativeConnectionProvider = null;
        LocalJmxConnectionProvider jmxConnectionProvider = null;

        try
        {
            Properties configuration = getConfiguration();

            ConnectionProperties connectionProperties = ConnectionProperties.from(configuration);

            LOG.info("Using connection properties {}", connectionProperties);

            nativeConnectionProvider = LocalNativeConnectionProvider.builder()
                    .withLocalhost(connectionProperties.getNativeHost())
                    .withPort(connectionProperties.getNativePort())
                    .build();
            jmxConnectionProvider = new LocalJmxConnectionProvider(connectionProperties.getJmxHost(), connectionProperties.getJmxPort());

            ECChronos ecChronos = new ECChronos(configuration, new LoggingFaultReporter(), nativeConnectionProvider, jmxConnectionProvider);

            start(ecChronos, foreground, nativeConnectionProvider, jmxConnectionProvider);
        }
        catch (Exception e)
        {
            LOG.error("Unable to start ecChronos", e);
            if (nativeConnectionProvider != null)
            {
                nativeConnectionProvider.close();
            }
            if (jmxConnectionProvider != null)
            {
                jmxConnectionProvider.close();
            }
        }
    }

    private static void start(ECChronos ecChronos, boolean foreground,
                              LocalNativeConnectionProvider nativeConnectionProvider,
                              LocalJmxConnectionProvider jmxConnectionProvider)
    {
        try
        {
            ecChronos.start();

            if (!foreground)
            {
                System.out.close();
                System.err.close();
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(ecChronos, nativeConnectionProvider, jmxConnectionProvider)));
        }
        catch (Exception e)
        {
            shutdown(ecChronos, nativeConnectionProvider, jmxConnectionProvider);
        }
    }

    private static Properties getConfiguration()
    {
        Properties configuration = new Properties();

        String configurationFile = System.getProperty(CONFIGURATION_FILE_PATH, DEFAULT_CONFIGURATION_FILE);

        try (FileInputStream configurationInputStream = new FileInputStream(configurationFile))
        {
            configuration.load(configurationInputStream);
        }
        catch (IOException e)
        {
            LOG.warn("Unable to load configuration file {}, using default values", configurationFile);
        }

        return configuration;
    }

    private static void shutdown(ECChronos ecChronos, LocalNativeConnectionProvider nativeConnectionProvider, LocalJmxConnectionProvider jmxConnectionProvider)
    {
        try
        {
            ecChronos.close();
            nativeConnectionProvider.close();
            jmxConnectionProvider.close();
        }
        catch (Exception e)
        {
            LOG.error("Unexpected exception while stopping", e);
        }
    }
}
