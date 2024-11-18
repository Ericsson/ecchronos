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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.CasLockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.DistributedJmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableStorageStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final CASLockFactory myLockFactory;

    public ECChronosInternals(
            final Config configuration,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final DistributedJmxConnectionProvider jmxConnectionProvider,
            final EccNodesSync eccNodesSync)
    {
        myJmxProxyFactory = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withEccNodesSync(eccNodesSync)
                .withNodesMap(generateNodesMap(nativeConnectionProvider.getNodes()))
                .build();

        CqlSession session = nativeConnectionProvider.getCqlSession();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myHostStatesImpl = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();

        myReplicatedTableProvider = new ReplicatedTableProviderImpl(
                session,
                myTableReferenceFactory,
                nativeConnectionProvider.getNodes());

        myTableStorageStatesImpl = TableStorageStatesImpl.builder()
                .withReplicatedTableProvider(myReplicatedTableProvider)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withConnectionProvider(nativeConnectionProvider)
                .build();

        myCassandraMetrics = new CassandraMetrics(myJmxProxyFactory);

        CasLockFactoryConfig casLockFactoryConfig = configuration.getLockFactory()
                .getCasLockFactoryConfig();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(nativeConnectionProvider)
                .withHostStates(myHostStatesImpl)
                .withKeyspaceName(casLockFactoryConfig.getKeyspaceName())
                .withCacheExpiryInSeconds(casLockFactoryConfig.getFailureCacheExpiryTimeInSeconds())
                .withConsistencySerial(casLockFactoryConfig.getConsistencySerial())
                .build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withRunInterval(configuration.getSchedulerConfig().getFrequency().getInterval(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS)
                .withNodeIDList(jmxConnectionProvider.getJmxConnections().keySet())
                .withLockFactory(myLockFactory)
                .build();
    }

    public final TableReferenceFactory getTableReferenceFactory()
    {
        return myTableReferenceFactory;
    }

    public final ReplicatedTableProvider getReplicatedTableProvider()
    {
        return myReplicatedTableProvider;
    }

    public final ScheduleManager getScheduleManager()
    {
        return myScheduleManagerImpl;
    }

    public final DistributedJmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    public final CassandraMetrics getCassandraMetrics()
    {
        return myCassandraMetrics;
    }

    public final TableRepairMetrics getTableRepairMetrics()
    {
        return NO_OP_REPAIR_METRICS;
    }

    public final HostStates getHostStates()
    {
        return myHostStatesImpl;
    }

    public final TableStorageStates getTableStorageStates()
    {
        return myTableStorageStatesImpl;
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

        myCassandraMetrics.close();
    }

    // In the future we should modify the DistributedNativeConnectionProvider
    // to generate this nodesMap instead of a nodesList
    private Map<UUID, Node> generateNodesMap(final List<Node> nodes)
    {
        Map<UUID, Node> nodesMap = new HashMap<>();
        nodes.forEach(node -> nodesMap.put(node.getHostId(), node));
        return nodesMap;
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

