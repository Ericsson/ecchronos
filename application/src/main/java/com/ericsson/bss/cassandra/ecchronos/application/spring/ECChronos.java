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

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.ECChronosInternals;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.*;
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
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;

    public ECChronos(ApplicationContext applicationContext, Config configuration, // NOPMD
            RepairFaultReporter repairFaultReporter, NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider, StatementDecorator statementDecorator,
            ReplicationState replicationState, RepairHistory repairHistory, RepairHistoryProvider repairHistoryProvider,
            MetricRegistry metricRegistry) throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider, jmxConnectionProvider,
                statementDecorator, metricRegistry);

        Session session = nativeConnectionProvider.getSession();
        Metadata metadata = session.getCluster().getMetadata();

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
                .construct(repairConfig.getProvider(), new Class[] { ApplicationContext.class }, applicationContext);

        myDefaultRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withCluster(session.getCluster())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withRepairConfiguration(repairConfigurationProvider::get)
                .withTableReferenceFactory(myECChronosInternals.getTableReferenceFactory())
                .build();

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withReplicationState(replicationState)
                .withRepairLockType(repairConfig.getLockType())
                .withMetadata(metadata)
                .withRepairConfiguration(repairConfig.asRepairConfiguration())
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
}
