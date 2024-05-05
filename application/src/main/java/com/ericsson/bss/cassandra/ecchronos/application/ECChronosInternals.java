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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.CasLockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.MetricInspector;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class ECChronosInternals implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronosInternals.class);
    private static final NoOpRepairMetrics NO_OP_REPAIR_METRICS = new NoOpRepairMetrics();
    private static final NoOpTableStorageState NO_OP_TABLE_STORAGE_STATE = new NoOpTableStorageState();

    private final ScheduleManagerImpl myScheduleManagerImpl;
    private final HostStatesImpl myHostStatesImpl;
    private final ReplicatedTableProviderImpl myReplicatedTableProvider;
    private final TableStorageStatesImpl myTableStorageStatesImpl;
    private final TableRepairMetricsImpl myTableRepairMetricsImpl;
    private final TableReferenceFactory myTableReferenceFactory;
    private final JmxProxyFactory myJmxProxyFactory;
    private final CASLockFactory myLockFactory;
    private final CassandraMetrics myCassandraMetrics;

    private final MetricInspector myMetricInspector;

    public ECChronosInternals(final Config configuration,
                              final NativeConnectionProvider nativeConnectionProvider,
                              final JmxConnectionProvider jmxConnectionProvider,
                              final StatementDecorator statementDecorator,
                              final MeterRegistry meterRegistry)
    {
        myJmxProxyFactory = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(jmxConnectionProvider)
                .build();

        myHostStatesImpl = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();

        CasLockFactoryConfig casLockFactoryConfig = configuration.getLockFactory()
                .getCasLockFactoryConfig();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(nativeConnectionProvider)
                .withHostStates(myHostStatesImpl)
                .withStatementDecorator(statementDecorator)
                .withKeyspaceName(casLockFactoryConfig.getKeyspaceName())
                .withCacheExpiryInSeconds(casLockFactoryConfig.getFailureCacheExpiryTimeInSeconds())
                .build();

        Node node = nativeConnectionProvider.getLocalNode();
        CqlSession session = nativeConnectionProvider.getSession();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myReplicatedTableProvider = new ReplicatedTableProviderImpl(node, session, myTableReferenceFactory);

        myCassandraMetrics = new CassandraMetrics(myJmxProxyFactory);

        if (configuration.getStatisticsConfig().isEnabled())
        {
            myTableStorageStatesImpl = TableStorageStatesImpl.builder()
                    .withReplicatedTableProvider(myReplicatedTableProvider)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .build();

            myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                    .withTableStorageStates(myTableStorageStatesImpl)
                    .withMeterRegistry(meterRegistry)
                    .build();

            myMetricInspector = new MetricInspector(meterRegistry,
                    configuration.getStatisticsConfig().getMyRepairFailureCountForReporting(),
                    configuration.getStatisticsConfig().getMyTimeWindowSizeinMinsForReporting());
            myMetricInspector.startInspection();
        }
        else
        {
            myTableStorageStatesImpl = null;
            myTableRepairMetricsImpl = null;
            myMetricInspector = null;
        }
        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(configuration.getSchedulerConfig().getFrequency().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .build();
    }

    public final TableReferenceFactory getTableReferenceFactory()
    {
        return myTableReferenceFactory;
    }

    public final HostStates getHostStates()
    {
        return myHostStatesImpl;
    }

    public final ReplicatedTableProvider getReplicatedTableProvider()
    {
        return myReplicatedTableProvider;
    }

    public final TableRepairMetrics getTableRepairMetrics()
    {
        if (myTableRepairMetricsImpl == null)
        {
            return NO_OP_REPAIR_METRICS;
        }

        return myTableRepairMetricsImpl;
    }

    public final ScheduleManager getScheduleManager()
    {
        return myScheduleManagerImpl;
    }

    public final JmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    public final TableStorageStates getTableStorageStates()
    {
        if (myTableStorageStatesImpl == null)
        {
            return NO_OP_TABLE_STORAGE_STATE;
        }
        return myTableStorageStatesImpl;
    }

    public final CassandraMetrics getCassandraMetrics()
    {
        return myCassandraMetrics;
    }

    public final boolean addRunPolicy(final RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.addRunPolicy(runPolicy);
    }

    public final boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.removeRunPolicy(runPolicy);
    }

    @Override
    public final void close()
    {
        myScheduleManagerImpl.close();

        if (myTableRepairMetricsImpl != null)
        {
            myTableRepairMetricsImpl.close();
        }
        if (myTableStorageStatesImpl != null)
        {
            myTableStorageStatesImpl.close();
        }

        myLockFactory.close();

        myHostStatesImpl.close();

        myCassandraMetrics.close();
    }

    private static class NoOpRepairMetrics implements TableRepairMetrics
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

    private static class NoOpTableStorageState implements TableStorageStates
    {
        @Override
        public long getDataSize(final TableReference tableReference)
        {
            return -1;
        }

        @Override
        public long getDataSize()
        {
            return -1;
        }
    }
}
