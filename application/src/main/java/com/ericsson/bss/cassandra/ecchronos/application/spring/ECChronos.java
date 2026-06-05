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

import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

/** Main ecChronos application lifecycle manager. */
@Configuration
public class ECChronos implements Closeable
{
    private final ECChronosInternals myECChronosInternals;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private final RepairStatsProvider myRepairStatsProvider;

    /**
     * Constructs a new ecChronos instance.
     * @param applicationContext the application context
     * @param configuration the application configuration
     * @param repairFaultReporter the repair fault reporter
     * @param nativeConnectionProvider the native connection provider
     * @param jmxConnectionProvider the JMX connection provider
     * @param statementDecorator the statement decorator
     * @param replicationState the replication state
     * @param repairHistory the repair history
     * @param repairHistoryProvider the repair history provider
     * @param defaultRepairConfigurationProvider the default repair configuration provider
     * @param eccCompositeMeterRegistry the ECC composite meter registry
     * @throws ConfigurationException if the configuration is invalid
     */
    @SuppressWarnings({"checkstyle:ParameterNumber", "PMD.ExcessiveParameterList"})
    public ECChronos(final ApplicationContext applicationContext,
                     final Config configuration,
                     final RepairFaultReporter repairFaultReporter,
                     final NativeConnectionProvider nativeConnectionProvider,
                     final JmxConnectionProvider jmxConnectionProvider,
                     final StatementDecorator statementDecorator,
                     final ReplicationState replicationState,
                     final RepairHistory repairHistory,
                     final RepairHistoryProvider repairHistoryProvider,
                     final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
                     final MeterRegistry eccCompositeMeterRegistry)
            throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(configuration, nativeConnectionProvider, jmxConnectionProvider,
                statementDecorator, eccCompositeMeterRegistry);

        CqlSession session = nativeConnectionProvider.getSession();

        GlobalRepairConfig repairConfig = configuration.getRepairConfig();

        RepairStateFactoryImpl repairStateFactoryImpl = RepairStateFactoryImpl.builder()
                .withReplicationState(replicationState)
                .withHostStates(myECChronosInternals.getHostStates())
                .withRepairHistoryProvider(repairHistoryProvider)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .build();

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(session)
                .withStatementDecorator(statementDecorator)
                .withKeyspaceName(configuration.getRunPolicy().getTimeBasedConfig().getKeyspaceName())
                .withLocalNode(nativeConnectionProvider.getLocalNode())
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withFaultReporter(repairFaultReporter)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withRepairStateFactory(repairStateFactoryImpl)
                .withReplicationState(replicationState)
                .withRepairLockType(repairConfig.getRepairLockType())
                .withTableStorageStates(myECChronosInternals.getTableStorageStates())
                .withRepairPolicies(Collections.singletonList(myTimeBasedRunPolicy))
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .withRepairHistory(repairHistory)
                .withCassandraMetrics(myECChronosInternals.getCassandraMetrics())
                .build();

        AbstractRepairConfigurationProvider repairConfigurationProvider = ReflectionUtils
                .construct(repairConfig.getRepairConfigurationClass(), new Class[] {
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
                .withRepairLockType(repairConfig.getRepairLockType())
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

    /**
     * Returns the table reference factory.
     * @return the table reference factory
     */
    @Bean
    public TableReferenceFactory tableReferenceFactory()
    {
        return myECChronosInternals.getTableReferenceFactory();
    }

    /**
     * Returns the on-demand repair scheduler.
     * @return the on demand repair scheduler
     */
    @Bean
    public OnDemandRepairScheduler onDemandRepairScheduler()
    {
        return myOnDemandRepairSchedulerImpl;
    }

    /**
     * Returns the repair scheduler.
     * @return the repair scheduler
     */
    @Bean(destroyMethod = "")
    public RepairScheduler repairScheduler()
    {
        return myRepairSchedulerImpl;
    }

    /**
     * Returns the replicated table provider.
     * @return the replicated table provider
     */
    @Bean
    public ReplicatedTableProvider replicatedTableProvider()
    {
        return myECChronosInternals.getReplicatedTableProvider();
    }

    /**
     * Returns the repair statistics provider.
     * @return the repair stats provider
     */
    @Bean
    public RepairStatsProvider repairStatsProvider()
    {
        return myRepairStatsProvider;
    }

    /**
     * Returns the time-based run policy.
     * @return the time based run policy
     */
    @Bean
    public TimeBasedRunPolicy timeBasedRunPolicy()
    {
        return myTimeBasedRunPolicy;
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
