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
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ThreadPoolTaskConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.JolokiaNotificationController;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.RepairStatsProviderImpl;
import org.springframework.beans.factory.annotation.Autowired;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaRefresher;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;

/** Main ecChronos application lifecycle manager. */
@Configuration
public class ECChronos implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronos.class);
    private final ECChronosInternals myECChronosInternals;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;
    private final OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private final RepairStatsProvider myRepairStatsProvider;
    private final NodeWorkerManager myNodeWorkerManager;

    /**
     * Constructs a new ecChronos instance, initializing the internal components, repair schedulers,
     * on-demand repair scheduler, and node worker manager.
     *
     * @param configuration
     *         the application configuration.
     * @param applicationContext
     *         the Spring application context.
     * @param nativeConnectionProvider
     *         the provider for Cassandra native connections.
     * @param jmxConnectionProvider
     *         the provider for JMX connections.
     * @param replicationState
     *         the replication state of the cluster.
     * @param defaultRepairConfigurationProvider
     *         the default repair configuration provider.
     * @param eccNodesSync
     *         the node synchronization instance.
     * @param repairHistoryService
     *         the repair history service.
     * @param repairFaultReporter
     *         the repair fault reporter.
     * @param eccCompositeMeterRegistry
     *         the meter registry for metrics.
     * @param ipTranslator
     *         the IP translator for address resolution.
     * @param notificationController
     *         the Jolokia notification controller, may be null.
     * @throws ConfigurationException
     *         if the configuration is invalid.
     */
    public ECChronos(//NOPMD long parameter list
            final Config configuration,
            final ApplicationContext applicationContext,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final DistributedJmxConnectionProvider jmxConnectionProvider,
            final ReplicationState replicationState,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final EccNodesSync eccNodesSync,
            final RepairHistoryService repairHistoryService,
            final RepairFaultReporter repairFaultReporter,
            final MeterRegistry eccCompositeMeterRegistry,
            final IpTranslator ipTranslator,
            @Autowired(required = false) @Nullable final JolokiaNotificationController notificationController) throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(
                configuration,
                nativeConnectionProvider,
                jmxConnectionProvider,
                eccNodesSync,
                eccCompositeMeterRegistry,
                ipTranslator,
                notificationController);

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

        SchemaRefresher schemaRefresher = new SchemaRefresher(
            myECChronosInternals.getReplicatedTableProvider(),
            myRepairSchedulerImpl,
            myECChronosInternals.getTableReferenceFactory(),
            repairConfigurationProvider::get,
            session,
            nativeConnectionProvider);

        LOG.debug("myNodeWorkerManager being created");
        myNodeWorkerManager = NodeWorkerManager.newBuilder()
                .withNativeConnection(nativeConnectionProvider)
                .withSchemaRefresher(schemaRefresher)
                .withThreadPool(setupThreadPool(threadPoolTaskConfig)).build();

        defaultRepairConfigurationProvider.fromBuilder(DefaultRepairConfigurationProvider.newBuilder()
                .withSession(session)
                .withEccNodesSync(eccNodesSync)
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withNodeWorkerManager(myNodeWorkerManager)
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withDistributedNativeConnectionProvider(nativeConnectionProvider)
                .withReplicaSetCache(repairStateFactoryImpl.getReplicaSetCache()));

        myRepairStatsProvider = new RepairStatsProviderImpl(
                nativeConnectionProvider,
                new VnodeRepairStateFactoryImpl(replicationState, repairHistoryService, true));
        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);

        Collection<UUID> nodeIDList = nativeConnectionProvider.getNodes().keySet();
        LOG.debug("Total nodes found: {}", nodeIDList.size());
        myECChronosInternals.getScheduleManager().createScheduleFutureForNodeIDList(nodeIDList);
    }

    /**
     * Returns the time-based run policy.
     * @return the time-based run policy
     */
    @Bean
    public TimeBasedRunPolicy timeBasedRunPolicy()
    {
        return myTimeBasedRunPolicy;
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
     * Returns the on-demand repair scheduler.
     * @return the on-demand repair scheduler
     */
    @Bean
    public OnDemandRepairScheduler onDemandRepairScheduler()
    {
        return myOnDemandRepairSchedulerImpl;
    }

    /**
     * Returns the schedule manager responsible for managing scheduled repair tasks.
     *
     * @return the {@link ScheduleManager} instance.
     */
    @Bean
    public ScheduleManager scheduleManager()
    {
        return myECChronosInternals.getScheduleManager();
    }

    /**
     * Returns the distributed JMX proxy factory, exposed for runtime configuration
     * of the repair max wait time.
     *
     * @return the {@link DistributedJmxProxyFactory} instance.
     */
    @Bean
    public DistributedJmxProxyFactory jmxProxyFactory()
    {
        return myECChronosInternals.getJmxProxyFactory();
    }

    /**
     * Returns the repair statistics provider for querying repair stats.
     *
     * @return the {@link RepairStatsProvider} instance.
     */
    @Bean
    public RepairStatsProvider repairStatsProvider()
    {
        return myRepairStatsProvider;
    }

    /**
     * Returns the node worker manager.
     * @return the node worker manager
     */
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


