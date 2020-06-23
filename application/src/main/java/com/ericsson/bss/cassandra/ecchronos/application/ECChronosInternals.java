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
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ECChronosInternals implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ECChronosInternals.class);

    private static final NoOpRepairMetrics NO_OP_REPAIR_METRICS = new NoOpRepairMetrics();

    private final ScheduleManagerImpl myScheduleManagerImpl;

    private final HostStatesImpl myHostStatesImpl;
    private final ReplicatedTableProviderImpl myReplicatedTableProvider;
    private final TableStorageStatesImpl myTableStorageStatesImpl;
    private final TableRepairMetricsImpl myTableRepairMetricsImpl;

    private final JmxProxyFactory myJmxProxyFactory;

    private final CASLockFactory myLockFactory;

    public ECChronosInternals(Properties configuration, NativeConnectionProvider nativeConnectionProvider,
                              JmxConnectionProvider jmxConnectionProvider, StatementDecorator statementDecorator)
            throws ConfigurationException
    {
        StatisticsProperties statisticsProperties = StatisticsProperties.from(configuration);

        myJmxProxyFactory = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(jmxConnectionProvider)
                .build();

        myHostStatesImpl = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();

        CASLockFactoryProperties casLockFactoryProperties = CASLockFactoryProperties.from(configuration);

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(nativeConnectionProvider)
                .withHostStates(myHostStatesImpl)
                .withStatementDecorator(statementDecorator)
                .withKeyspaceName(casLockFactoryProperties.getKeyspaceName())
                .build();

        Host host = nativeConnectionProvider.getLocalHost();
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();

        myReplicatedTableProvider = new ReplicatedTableProviderImpl(host, metadata);

        if (statisticsProperties.isEnabled())
        {
            myTableStorageStatesImpl = TableStorageStatesImpl.builder()
                    .withReplicatedTableProvider(myReplicatedTableProvider)
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .build();

            myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                    .withTableStorageStates(myTableStorageStatesImpl)
                    .withStatisticsDirectory(statisticsProperties.getStatisticsDirectory().toString())
                    .build();
        }
        else
        {
            myTableStorageStatesImpl = null;
            myTableRepairMetricsImpl = null;
        }

        SchedulerProperties schedulerProperties = SchedulerProperties.from(configuration);

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(schedulerProperties.getRunInterval(), schedulerProperties.getTimeUnit())
                .build();
    }

    public HostStates getHostStates()
    {
        return myHostStatesImpl;
    }

    public ReplicatedTableProvider getReplicatedTableProvider()
    {
        return myReplicatedTableProvider;
    }

    public TableRepairMetrics getTableRepairMetrics()
    {
        if (myTableStorageStatesImpl == null)
        {
            return NO_OP_REPAIR_METRICS;
        }

        return myTableRepairMetricsImpl;
    }

    public ScheduleManager getScheduleManager()
    {
        return myScheduleManagerImpl;
    }

    public JmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    public TableStorageStates getTableStorageStates()
    {
        return myTableStorageStatesImpl;
    }

    public boolean addRunPolicy(RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.addRunPolicy(runPolicy);
    }

    public boolean removeRunPolicy(RunPolicy runPolicy)
    {
        return myScheduleManagerImpl.removeRunPolicy(runPolicy);
    }

    @Override
    public void close()
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
    }

    private static class NoOpRepairMetrics implements TableRepairMetrics
    {

        @Override
        public void repairState(TableReference tableReference, int repairedRanges, int notRepairedRanges)
        {
            LOG.debug("Updated repair state of {}, {}/{} repaired ranges", tableReference, repairedRanges, notRepairedRanges);
        }

        @Override
        public void lastRepairedAt(TableReference tableReference, long lastRepairedAt)
        {
            LOG.debug("Table {} last repaired at {}", tableReference, lastRepairedAt);
        }

        @Override
        public void repairTiming(TableReference tableReference, long timeTaken, TimeUnit timeUnit, boolean successful)
        {
            if (LOG.isTraceEnabled())
            {
                LOG.trace("Repair timing for table {} {}ms, it was ", tableReference,
                        timeUnit.toMillis(timeTaken), successful ? "successful" : "not successful");
            }
        }
    }
}
