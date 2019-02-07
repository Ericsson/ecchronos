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
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;

public class ECChronos implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronos.class);

    private static final String CONFIGURATION_FILE_PATH = "ecchronos.config";
    private static final String DEFAULT_CONFIGURATION_FILE = "./ecChronos.cfg";

    private final ECChronosInternals myECChronosInternals;

    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final RESTServer myRestServer;

    public ECChronos(Properties configuration, RepairFaultReporter repairFaultReporter,
                     NativeConnectionProvider nativeConnectionProvider,
                     JmxConnectionProvider jmxConnectionProvider,
                     StatementDecorator statementDecorator) throws ConfigurationException
    {
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

        RepairProperties repairProperties = RepairProperties.from(configuration);

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withFaultReporter(repairFaultReporter)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withRepairStateFactory(repairStateFactoryImpl)
                .withRepairLockType(repairProperties.getRepairLockType())
                .build();

        myDefaultRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withCluster(nativeConnectionProvider.getSession().getCluster())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withDefaultRepairConfiguration(getRepairConfiguration(repairProperties))
                .build();

        RESTServerProperties restServerProperties = RESTServerProperties.from(configuration);

        myRestServer = new RESTServer(myRepairSchedulerImpl, myECChronosInternals.getScheduleManager(), restServerProperties.getAddress());
    }

    public void start() throws RESTServerException
    {
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
        myRestServer.start();
    }

    @Override
    public void close()
    {
        myRestServer.close();

        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);

        myTimeBasedRunPolicy.close();
        myDefaultRepairConfigurationProvider.close();
        myRepairSchedulerImpl.close();

        myECChronosInternals.close();
    }

    private RepairConfiguration getRepairConfiguration(RepairProperties repairProperties)
    {
        LOG.debug("Using repair properties {}", repairProperties);

        return RepairConfiguration.newBuilder()
                .withRepairInterval(repairProperties.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withParallelism(repairProperties.getRepairParallelism())
                .withRepairWarningTime(repairProperties.getRepairAlarmWarnInMs(), TimeUnit.MILLISECONDS)
                .withRepairErrorTime(repairProperties.getRepairAlarmErrorInMs(), TimeUnit.MILLISECONDS)
                .withRepairUnwindRatio(repairProperties.getRepairUnwindRatio())
                .build();
    }

    public static void main(String[] args) throws IOException
    {
        boolean foreground = false;

        if (args.length >= 1 && "-f".equals(args[0])) // NOPMD
        {
            foreground = true;
        }

        NativeConnectionProvider nativeConnectionProvider = null;
        JmxConnectionProvider jmxConnectionProvider = null;
        StatementDecorator statementDecorator;

        try
        {
            Properties configuration = getConfiguration();

            ConnectionProperties connectionProperties = ConnectionProperties.from(configuration);

            LOG.info("Using connection properties {}", connectionProperties);

            nativeConnectionProvider = getNativeConnectionProvider(configuration, connectionProperties);
            jmxConnectionProvider = getJmxConnectionProvider(configuration, connectionProperties);
            statementDecorator = getStatementDecorator(configuration, connectionProperties);

            ECChronos ecChronos = new ECChronos(configuration, new LoggingFaultReporter(), nativeConnectionProvider, jmxConnectionProvider, statementDecorator);

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
                              NativeConnectionProvider nativeConnectionProvider,
                              JmxConnectionProvider jmxConnectionProvider)
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

    private static void shutdown(ECChronos ecChronos, NativeConnectionProvider nativeConnectionProvider, JmxConnectionProvider jmxConnectionProvider)
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

    private static NativeConnectionProvider getNativeConnectionProvider(Properties configuration, ConnectionProperties connectionProperties) throws ConfigurationException
    {
        return ReflectionUtils.construct(connectionProperties.getNativeConnectionProviderClass(), configuration);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(Properties configuration, ConnectionProperties connectionProperties) throws ConfigurationException
    {
        return ReflectionUtils.construct(connectionProperties.getJmxConnectionProviderClass(), configuration);
    }

    private static StatementDecorator getStatementDecorator(Properties configuration, ConnectionProperties connectionProperties) throws ConfigurationException
    {
        return ReflectionUtils.construct(connectionProperties.getStatementDecoratorClass(), configuration);
    }
}
