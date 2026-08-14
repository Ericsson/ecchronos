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
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.CasLockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.DistributedJmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.JolokiaNotificationController;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairLockFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableStorageStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.MetricInspector;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal components of the ecChronos application. Manages the lifecycle of core services including
 * the schedule manager, lock factory, JMX proxy factory, host states, table storage states,
 * and repair metrics. Implements {@link Closeable} to ensure proper cleanup of all resources.
 */
public class ECChronosInternals implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronosInternals.class);
    private static final NoOpRepairMetrics NO_OP_REPAIR_METRICS = new NoOpRepairMetrics();

    private final ScheduleManagerImpl myScheduleManagerImpl;
    private final ReplicatedTableProviderImpl myReplicatedTableProvider;
    private final TableReferenceFactory myTableReferenceFactory;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final CassandraMetrics myCassandraMetrics;
    private final HostStatesImpl myHostStatesImpl;
    private final TableStorageStatesImpl myTableStorageStatesImpl;
    private final TableRepairMetricsImpl myTableRepairMetricsImpl;
    private final CASLockFactory myLockFactory;
    private final MetricInspector myMetricInspector;
    private final JolokiaNotificationController myJolokiaNotificationController;

    /**
     * Constructs a new {@code ECChronosInternals} instance, initializing the JMX proxy factory,
     * host states, replicated table provider, lock factory, schedule manager, and optionally
     * metrics components based on the provided configuration.
     *
     * @param configuration
     *         the application configuration.
     * @param nativeConnectionProvider
     *         the provider for Cassandra native connections.
     * @param jmxConnectionProvider
     *         the provider for JMX connections.
     * @param eccNodesSync
     *         the node synchronization instance.
     * @param meterRegistry
     *         the meter registry for metrics.
     * @param ipTranslator
     *         the IP translator for address resolution.
     * @param notificationController
     *         the Jolokia notification controller, may be null.
     */
    public ECChronosInternals(
            final Config configuration,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final DistributedJmxConnectionProvider jmxConnectionProvider,
            final EccNodesSync eccNodesSync,
            final MeterRegistry meterRegistry,
            final IpTranslator ipTranslator,
            final JolokiaNotificationController notificationController
    )
    {
        myJolokiaNotificationController = notificationController;
        myJmxProxyFactory = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withEccNodesSync(eccNodesSync)
                .withNodesMap(nativeConnectionProvider.getNodes())
                .withJolokiaEnabled(configuration
                        .getConnectionConfig().getJmxConnection().getJolokiaConfig().isEnabled())
                .withMaxWaitTimeInMinutes(configuration.getConnectionConfig().getJmxConnection().getMaxWaitTimeInMinutes())
                .withIpTranslator(ipTranslator)
                .withJolokiaNotificationController(myJolokiaNotificationController)
                .build();

        CqlSession session = nativeConnectionProvider.getCqlSession();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        if (!configuration.getConnectionConfig().getJmxConnection().getUseBroadcastRPCAddress())
        {
            ipTranslator.init(session);
        }

        myHostStatesImpl = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();

        myReplicatedTableProvider = new ReplicatedTableProviderImpl(
                session,
                myTableReferenceFactory,
                nativeConnectionProvider);

        myCassandraMetrics = new CassandraMetrics(myJmxProxyFactory);

        if (configuration.getStatisticsConfig().isEnabled())
        {
            myTableStorageStatesImpl = TableStorageStatesImpl.builder()
                    .withReplicatedTableProvider(myReplicatedTableProvider)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withConnectionProvider(nativeConnectionProvider)
                    .build();

            myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                    .withMeterRegistry(meterRegistry)
                    .build();

            myMetricInspector = new MetricInspector(meterRegistry,
                    configuration.getStatisticsConfig().getRepairFailuresCount(),
                    configuration.getStatisticsConfig().getRepairFailuresTimeWindow()
                            .getInterval(TimeUnit.MINUTES),
                    configuration.getStatisticsConfig().getTriggerIntervalForMetricInspection()
                            .getInterval(TimeUnit.MILLISECONDS));
            myMetricInspector.startInspection();
        }
        else
        {
            myTableStorageStatesImpl = null;
            myTableRepairMetricsImpl = null;
            myMetricInspector = null;
        }

        CasLockFactoryConfig casLockFactoryConfig = configuration.getLockFactory()
                .getCasLockFactoryConfig();

        RepairLockFactoryImpl.configure(casLockFactoryConfig.getLocksPerResource());

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(nativeConnectionProvider)
                .withKeyspaceName(casLockFactoryConfig.getKeyspaceName())
                .withCacheExpiryInSeconds(casLockFactoryConfig.getFailureCacheExpiryTimeInSeconds())
                .withConsistencySerial(casLockFactoryConfig.getConsistencySerial())
                .build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withRunInterval(configuration.getSchedulerConfig().getFrequency().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withSessionWindow(configuration.getSchedulerConfig().getSessionWindow().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withCooldown(configuration.getSchedulerConfig().getCooldown().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withNodeIDList(jmxConnectionProvider.getJmxConnections().keySet())
                .withNativeConnectionProvider(nativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .build();
    }

    /**
     * Returns the table reference factory used to create table references.
     *
     * @return the {@link TableReferenceFactory} instance.
     */
    public final TableReferenceFactory getTableReferenceFactory()
    {
        return myTableReferenceFactory;
    }

    /**
     * Returns the replicated table provider used to discover replicated tables.
     *
     * @return the {@link ReplicatedTableProvider} instance.
     */
    public final ReplicatedTableProvider getReplicatedTableProvider()
    {
        return myReplicatedTableProvider;
    }

    /**
     * Returns the schedule manager responsible for managing scheduled repair tasks.
     *
     * @return the {@link ScheduleManager} instance.
     */
    public final ScheduleManager getScheduleManager()
    {
        return myScheduleManagerImpl;
    }

    /**
     * Returns the distributed JMX proxy factory used to create JMX proxies for cluster nodes.
     *
     * @return the {@link DistributedJmxProxyFactory} instance.
     */
    public final DistributedJmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    /**
     * Returns the Cassandra metrics instance for accessing node-level metrics via JMX.
     *
     * @return the {@link CassandraMetrics} instance.
     */
    public final CassandraMetrics getCassandraMetrics()
    {
        return myCassandraMetrics;
    }

    /**
     * Returns the table repair metrics.
     * @return the table repair metrics, or a no-op implementation if unavailable
     */
    public final TableRepairMetrics getTableRepairMetrics()
    {
        return Objects.requireNonNullElse(myTableRepairMetricsImpl, NO_OP_REPAIR_METRICS);
    }

    /**
     * Returns the host states.
     * @return the host states
     */
    public final HostStates getHostStates()
    {
        return myHostStatesImpl;
    }

    /**
     * Returns the table storage states.
     * @return the table storage states
     */
    public final TableStorageStates getTableStorageStates()
    {
        return myTableStorageStatesImpl;
    }

    /**
     * Adds a run policy to the schedule manager.
     * @param runPolicy the run policy to add
     * @return true if the policy was added successfully
     */
    public final boolean addRunPolicy(final RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.addRunPolicy(runPolicy);
    }

    /**
     * Removes a run policy from the schedule manager.
     * @param runPolicy the run policy to remove
     * @return true if the policy was removed successfully
     */
    public final boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.removeRunPolicy(runPolicy);
    }

    @Override
    public final void close()
    {
        myScheduleManagerImpl.close();

        myCassandraMetrics.close();

        if (myMetricInspector != null)
        {
            myMetricInspector.stopInspection();
        }

        if (myTableRepairMetricsImpl != null)
        {
            myTableRepairMetricsImpl.close();
        }

        if (myTableStorageStatesImpl != null)
        {
            myTableStorageStatesImpl.close();
        }

        myHostStatesImpl.close();
        myLockFactory.close();

        if (myJolokiaNotificationController != null)
        {
            myJolokiaNotificationController.close();
        }
    }

    private static final class NoOpRepairMetrics implements TableRepairMetrics
    {

        @Override
        public void repairState(final TableReference tableReference,
                final int repairedRanges,
                final int notRepairedRanges)
        {
            LOG.trace("Updated repair state of {}, {}/{} repaired ranges", tableReference, repairedRanges,
                    notRepairedRanges);
        }

        @Override
        public void lastRepairedAt(final TableReference tableReference, final long lastRepairedAt)
        {
            LOG.debug("Table {} last repaired at {}", tableReference, lastRepairedAt);
        }

        @Override
        public void remainingRepairTime(final TableReference tableReference, final long remainingRepairTime)
        {
            LOG.debug("Table {} remaining repair time {}", tableReference, remainingRepairTime);
        }

        @Override
        public void repairSession(final TableReference tableReference,
                final long timeTaken,
                final TimeUnit timeUnit,
                final boolean successful)
        {
            if (LOG.isTraceEnabled())
            {
                LOG.trace("Repair timing for table {} {}ms, it was {}", tableReference,
                        timeUnit.toMillis(timeTaken), successful ? "successful" : "not successful");
            }
        }
    }
}

