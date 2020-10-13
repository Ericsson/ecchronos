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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.application.ECChronosInternals;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.*;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

@Configuration
public class ECChronos implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronos.class);

    private final ECChronosInternals myECChronosInternals;

    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;

    public ECChronos(Config configuration, RepairFaultReporter repairFaultReporter,
            NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider,
            StatementDecorator statementDecorator)
    {
        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider, jmxConnectionProvider,
                statementDecorator);

        Host host = nativeConnectionProvider.getLocalHost();
        Session session = nativeConnectionProvider.getSession();
        Metadata metadata = session.getCluster().getMetadata();

        Config.RepairConfig repairConfig = configuration.getRepair();

        NodeResolver nodeResolver = new NodeResolverImpl(metadata);
        Node localNode = nodeResolver.fromUUID(host.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(nodeResolver, metadata, host);

        RepairHistory repairHistory;
        RepairHistoryProvider repairHistoryProvider;

        if (repairConfig.getHistory().getProvider() == Config.RepairHistory.Provider.CASSANDRA)
        {
            repairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver,
                    session, statementDecorator,
                    repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS));
            repairHistory = RepairHistory.NO_OP;
        }
        else
        {
            EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                    .withSession(session)
                    .withReplicationState(replicationState)
                    .withLocalNode(localNode)
                    .withStatementDecorator(statementDecorator)
                    .withLookbackTime(repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS),
                            TimeUnit.MILLISECONDS)
                    .withKeyspace(repairConfig.getHistory().getKeyspace())
                    .build();

            if (repairConfig.getHistory().getProvider() == Config.RepairHistory.Provider.UPGRADE)
            {
                repairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver,
                        session, statementDecorator,
                        repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS));
            }
            else
            {
                repairHistoryProvider = eccRepairHistory;
            }

            repairHistory = eccRepairHistory;
        }

        RepairStateFactoryImpl repairStateFactoryImpl = RepairStateFactoryImpl.builder()
                .withReplicationState(replicationState)
                .withHostStates(myECChronosInternals.getHostStates())
                .withRepairHistoryProvider(repairHistoryProvider)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .build();

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(session)
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
                .withRepairHistory(repairHistory)
                .build();
        RepairConfiguration repairConfiguration = getRepairConfiguration(repairConfig);
        myDefaultRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withCluster(session.getCluster())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withDefaultRepairConfiguration(repairConfiguration)
                .withTableReferenceFactory(myECChronosInternals.getTableReferenceFactory())
                .build();

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withReplicationState(replicationState)
                .withRepairLockType(repairConfig.getLockType())
                .withMetadata(metadata)
                .withRepairConfiguration(repairConfiguration)
                .withRepairHistory(repairHistory)
                .build();
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
    }

    @Bean
    public TableReferenceFactory tableReferenceFactory()
    {
        return myECChronosInternals.getTableReferenceFactory();
    }

    @Bean
    public OnDemandRepairScheduler onDemandRepairScheduler()
    {
        return myOnDemandRepairSchedulerImpl;
    }

    @Bean(destroyMethod = "")
    public RepairScheduler repairScheduler()
    {
        return myRepairSchedulerImpl;
    }

    @Override
    public void close()
    {
        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);

        myTimeBasedRunPolicy.close();
        myDefaultRepairConfigurationProvider.close();
        myRepairSchedulerImpl.close();
        myOnDemandRepairSchedulerImpl.close();

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
}
