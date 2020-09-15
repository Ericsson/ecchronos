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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ECChronos implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronos.class);

    private static final String CONFIGURATION_FILE_PATH = "ecchronos.config";
    private static final String DEFAULT_CONFIGURATION_FILE = "./ecc.yml";

    private final ECChronosInternals myECChronosInternals;

    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairScheduler;
    private final HTTPServer myHttpServer;

    public ECChronos(Config configuration, RepairFaultReporter repairFaultReporter,
            NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider,
            StatementDecorator statementDecorator)
    {
        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider, jmxConnectionProvider,
                statementDecorator);

        Host host = nativeConnectionProvider.getLocalHost();
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();

        Config.RepairConfig repairConfig = configuration.getRepair();

        RepairHistoryProviderImpl repairHistoryProvider = new RepairHistoryProviderImpl(
                nativeConnectionProvider.getSession(), statementDecorator,
                repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS));

        RepairStateFactoryImpl repairStateFactoryImpl = RepairStateFactoryImpl.builder()
                .withMetadata(metadata)
                .withHost(host)
                .withHostStates(myECChronosInternals.getHostStates())
                .withRepairHistoryProvider(repairHistoryProvider)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .build();

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(nativeConnectionProvider.getSession())
                .withStatementDecorator(statementDecorator)
                .withKeyspaceName(configuration.getRunPolicy().getTimeBased().getKeyspace())
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withFaultReporter(repairFaultReporter)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withRepairStateFactory(repairStateFactoryImpl)
                .withRepairLockType(repairConfig.getLockType())
                .withTableStorageStates(myECChronosInternals.getTableStorageStates())
                .withRepairPolicies(Collections.singletonList(myTimeBasedRunPolicy))
                .build();
        RepairConfiguration repairConfiguration = getRepairConfiguration(repairConfig);
        myDefaultRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withCluster(nativeConnectionProvider.getSession().getCluster())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withDefaultRepairConfiguration(repairConfiguration)
                .build();

        ReplicationState replicationState = new ReplicationState(metadata, host);
        myOnDemandRepairScheduler = OnDemandRepairSchedulerImpl.builder()
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withReplicationState(replicationState)
                .withRepairLockType(repairConfig.getLockType())
                .withMetadata(metadata)
                .withRepairConfiguration(repairConfiguration)
                .build();

        InetSocketAddress restAddress = new InetSocketAddress(configuration.getRestServer().getHost(),
                configuration.getRestServer().getPort());

        myHttpServer = new HTTPServer(myRepairSchedulerImpl, myOnDemandRepairScheduler,
                myECChronosInternals.getScheduleManager(), restAddress);
    }

    public void start() throws HTTPServerException
    {
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
        myHttpServer.start();
    }

    @Override
    public void close()
    {
        myHttpServer.close();

        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);

        myTimeBasedRunPolicy.close();
        myDefaultRepairConfigurationProvider.close();
        myRepairSchedulerImpl.close();
        myOnDemandRepairScheduler.close();

        myECChronosInternals.close();
    }

    private RepairConfiguration getRepairConfiguration(Config.RepairConfig repairProperties)
    {
        LOG.debug("Using repair properties {}", repairProperties);

        return RepairConfiguration.newBuilder()
                .withRepairInterval(repairProperties.getInterval().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withParallelism(repairProperties.getParallelism())
                .withRepairWarningTime(repairProperties.getAlarm().getWarn().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairErrorTime(repairProperties.getAlarm().getError().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withRepairUnwindRatio(repairProperties.getUnwindRatio())
                .withTargetRepairSizeInBytes(repairProperties.getSizeTargetInBytes())
                .build();
    }

    public static void main(String[] args) throws IOException
    {
        NativeConnectionProvider nativeConnectionProvider = null;
        JmxConnectionProvider jmxConnectionProvider = null;
        StatementDecorator statementDecorator;

        try
        {
            Config configuration = getConfiguration();

            LOG.info("Using connection properties {}", configuration.getConnectionConfig());

            nativeConnectionProvider = getNativeConnectionProvider(configuration); // NOPMD
            jmxConnectionProvider = getJmxConnectionProvider(configuration); // NOPMD
            statementDecorator = getStatementDecorator(configuration);

            ECChronos ecChronos = new ECChronos(configuration, new LoggingFaultReporter(), nativeConnectionProvider,
                    jmxConnectionProvider, statementDecorator);

            start(ecChronos, nativeConnectionProvider, jmxConnectionProvider);
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

    private static void start(ECChronos ecChronos, NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider)
    {
        try
        {
            ecChronos.start();

            Runtime.getRuntime().addShutdownHook(
                    new Thread(() -> shutdown(ecChronos, nativeConnectionProvider, jmxConnectionProvider)));
        }
        catch (Exception e)
        {
            LOG.error("Unexpected exception during start", e);
            shutdown(ecChronos, nativeConnectionProvider, jmxConnectionProvider);
        }
    }

    private static Config getConfiguration() throws ConfigurationException
    {
        String configurationFile = System.getProperty(CONFIGURATION_FILE_PATH, DEFAULT_CONFIGURATION_FILE);
        File file = new File(configurationFile);

        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            return objectMapper.readValue(file, Config.class);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file " + file, e);
        }
    }

    private static void shutdown(ECChronos ecChronos, NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider)
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

    private static NativeConnectionProvider getNativeConnectionProvider(Config configuration)
            throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getProviderClass(), configuration);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getJmx().getProviderClass(), configuration);
    }

    private static StatementDecorator getStatementDecorator(Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getDecoratorClass(), configuration);
    }
}
