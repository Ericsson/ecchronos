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

import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.ECChronosInternals;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

@Configuration
public class ECChronos implements Closeable
{
    private final ECChronosInternals myECChronosInternals;

    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private final RepairStatsProvider myRepairStatsProvider;

    public ECChronos(final ApplicationContext applicationContext, // NOPMD
                     final Config configuration,
                     final RepairFaultReporter repairFaultReporter,
                     final NativeConnectionProvider nativeConnectionProvider,
                     final JmxConnectionProvider jmxConnectionProvider,
                     final StatementDecorator statementDecorator,
                     final ReplicationState replicationState,
                     final RepairHistory repairHistory,
                     final RepairHistoryProvider repairHistoryProvider,
                     final MetricRegistry metricRegistry,
                     final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider)
            throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider, jmxConnectionProvider,
                statementDecorator, metricRegistry);

        CqlSession session = nativeConnectionProvider.getSession();

        Config.GlobalRepairConfig repairConfig = configuration.getRepair();

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

        AbstractRepairConfigurationProvider repairConfigurationProvider = ReflectionUtils
                .construct(repairConfig.getProvider(), new Class[] {
                        ApplicationContext.class
                }, applicationContext);

        defaultRepairConfigurationProvider.fromBuilder(DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withSession(session)
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withRepairConfiguration(repairConfigurationProvider::get)
                .withTableReferenceFactory(myECChronosInternals.getTableReferenceFactory()));

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withReplicationState(replicationState)
                .withRepairLockType(repairConfig.getLockType())
                .withSession(session)
                .withRepairConfiguration(repairConfig.asRepairConfiguration())
                .withRepairHistory(repairHistory)
                .withOnDemandStatus(new OnDemandStatus(nativeConnectionProvider))
                .build();
        myRepairStatsProvider = new RepairStatsProviderImpl(new VnodeRepairStateFactoryImpl(replicationState,
                repairHistoryProvider,
                true));
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

    @Bean
    public ReplicatedTableProvider replicatedTableProvider()
    {
        return myECChronosInternals.getReplicatedTableProvider();
    }

    @Bean
    public RepairStatsProvider repairStatsProvider()
    {
        return myRepairStatsProvider;
    }

    @Override
    public final void close()
    {
        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);

        myTimeBasedRunPolicy.close();
        myRepairSchedulerImpl.close();
        myOnDemandRepairSchedulerImpl.close();

        myECChronosInternals.close();
    }
}
