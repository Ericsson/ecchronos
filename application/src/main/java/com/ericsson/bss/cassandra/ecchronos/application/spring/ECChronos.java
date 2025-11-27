/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ThreadPoolTaskConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.RepairStatsProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;

@Configuration
public class ECChronos implements Closeable
{
    private final ECChronosInternals myECChronosInternals;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private final RepairStatsProvider myRepairStatsProvider;
    private final NodeWorkerManager myNodeWorkerManager;

    public ECChronos(
            final Config configuration,
            final ApplicationContext applicationContext,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final DistributedJmxConnectionProvider jmxConnectionProvider,
            final ReplicationState replicationState,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final EccNodesSync eccNodesSync,
            final RepairHistoryService repairHistoryService,
            final RepairFaultReporter repairFaultReporter,
            final MeterRegistry eccCompositeMeterRegistry
            )
            throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(
                configuration, nativeConnectionProvider, jmxConnectionProvider, eccNodesSync, eccCompositeMeterRegistry);

        CqlSession session = nativeConnectionProvider.getCqlSession();

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(session)
                .withKeyspaceName(configuration.getRunPolicy().getTimeBasedConfig().getKeyspaceName())
                .build();

        RepairStateFactoryImpl repairStateFactoryImpl = RepairStateFactoryImpl.builder()
                .withReplicationState(replicationState)
                .withHostStates(myECChronosInternals.getHostStates())
                .withRepairHistoryProvider(repairHistoryService)
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withCassandraMetrics(myECChronosInternals.getCassandraMetrics())
                .withReplicationState(replicationState)
                .withRepairPolicies(Collections.singletonList(myTimeBasedRunPolicy))
                .withCassandraMetrics(myECChronosInternals.getCassandraMetrics())
                .withRepairStateFactory(repairStateFactoryImpl)
                .withRepairHistory(repairHistoryService)
                .withFaultReporter(repairFaultReporter)
                .withTableStorageStates(myECChronosInternals.getTableStorageStates())
                .withRepairLockType(configuration.getRepairConfig().getRepairLockType())
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build();

        AbstractRepairConfigurationProvider repairConfigurationProvider = new FileBasedRepairConfiguration(applicationContext);

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withReplicationState(replicationState)
                .withRepairLockType(configuration.getRepairConfig().getRepairLockType())
                .withSession(session)
                .withRepairConfigurationFunction(configuration.getRepairConfig().asRepairConfiguration())
                .withRepairHistory(repairHistoryService)
                .withRepairConfigurationFunction(repairConfigurationProvider::get)
                .withOnDemandStatus(new OnDemandStatus(nativeConnectionProvider))
                .build();

        ThreadPoolTaskConfig threadPoolTaskConfig = configuration.getConnectionConfig().getThreadPoolTaskConfig();

        myNodeWorkerManager = NodeWorkerManager.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withRepairConfiguration(repairConfigurationProvider::get)
                .withNativeConnection(nativeConnectionProvider)
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withTableReferenceFactory(myECChronosInternals.getTableReferenceFactory())
                .withThreadPool(setupThreadPool(threadPoolTaskConfig)).build();

        defaultRepairConfigurationProvider.fromBuilder(DefaultRepairConfigurationProvider.newBuilder()
                .withSession(session)
                .withEccNodesSync(eccNodesSync)
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withNodeWorkerManager(myNodeWorkerManager)
                .withDistributedNativeConnectionProvider(nativeConnectionProvider));

        myRepairStatsProvider = new RepairStatsProviderImpl(
                nativeConnectionProvider,
                new VnodeRepairStateFactoryImpl(replicationState, repairHistoryService, true));
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
    }

    @Bean
    public TimeBasedRunPolicy timeBasedRunPolicy()
    {
        return myTimeBasedRunPolicy;
    }

    @Bean
    public TableReferenceFactory tableReferenceFactory()
    {
        return myECChronosInternals.getTableReferenceFactory();
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
    public OnDemandRepairScheduler onDemandRepairScheduler()
    {
        return myOnDemandRepairSchedulerImpl;
    }

    @Bean
    public RepairStatsProvider repairStatsProvider()
    {
        return myRepairStatsProvider;
    }

    @Bean
    public NodeWorkerManager nodeWorkerManager()
    {
        return myNodeWorkerManager;
    }

    @Override
    public final void close()
    {
        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);
        myTimeBasedRunPolicy.close();
        myRepairSchedulerImpl.close();
        myECChronosInternals.close();
        myOnDemandRepairSchedulerImpl.close();
        myNodeWorkerManager.shutdown();
    }

    private ThreadPoolTaskExecutor setupThreadPool(final ThreadPoolTaskConfig threadPoolTaskConfig)
    {
        ThreadPoolTaskExecutor threadPool = new ThreadPoolTaskExecutor();
        threadPool.setCorePoolSize(threadPoolTaskConfig.getCorePoolSize());
        threadPool.setMaxPoolSize(threadPoolTaskConfig.getMaxPoolSize());
        threadPool.setQueueCapacity(threadPoolTaskConfig.getQueueCapacity());
        threadPool.setKeepAliveSeconds(threadPoolTaskConfig.getKeepAliveSeconds());
        threadPool.setThreadNamePrefix("NodeWorker-");
        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        return threadPool;
    }
}


