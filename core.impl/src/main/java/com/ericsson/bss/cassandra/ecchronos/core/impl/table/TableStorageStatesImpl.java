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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * Implementation of {@link TableStorageStates} that periodically fetches table sizes
 * from each node via JMX and makes them available for querying.
 */
public final class TableStorageStatesImpl implements TableStorageStates, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TableStorageStatesImpl.class);

    private static final long DEFAULT_UPDATE_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(60);

    private final AtomicReference<Map<UUID, ImmutableMap<TableReference, Long>>> myTableSizes = new AtomicReference<>();
    private final ScheduledExecutorService myScheduledExecutorService;

    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private TableStorageStatesImpl(final Builder builder)
    {
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myNativeConnectionProvider = builder.myNativeConnectionProvider;

        initializeEmptyTableSizeMap();

        myScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("TableStateUpdater-%d").build());
        myScheduledExecutorService.scheduleAtFixedRate(this::updateTableStates,
                builder.myInitialDelayInMs,
                builder.myUpdateDelayInMs,
                TimeUnit.MILLISECONDS);
    }

    private void initializeEmptyTableSizeMap()
    {
        for (Node node : myNativeConnectionProvider.getNodes().values())
        {
            UUID nodeID = node.getHostId();
            initializeEmptyNodeMap(nodeID);
        }
    }

    private void initializeEmptyNodeMap(final UUID nodeID)
    {
        Map<UUID, ImmutableMap<TableReference, Long>> emptyNewEntry = new HashMap<>();
        Map<TableReference, Long> emptyDataSizes = new HashMap<>();
        ImmutableMap<TableReference, Long> emptyTableSize = ImmutableMap.copyOf(emptyDataSizes);
        emptyNewEntry.put(nodeID, emptyTableSize);
        myTableSizes.set(emptyNewEntry);
    }

    @Override
    public long getDataSize(final UUID nodeID, final TableReference tableReference)
    {
        Map<UUID, ImmutableMap<TableReference, Long>> dataSizes = myTableSizes.get();

        if (!dataSizes.containsKey(nodeID))
        {
            initializeEmptyNodeMap(nodeID);
        }

        if (dataSizes.get(nodeID).containsKey(tableReference))
        {
                return dataSizes.get(nodeID).get(tableReference);
        }
        return 0;
    }

    @Override
    public long getDataSize(final UUID nodeID)
    {
        Map<UUID, ImmutableMap<TableReference, Long>> dataSizesMap = myTableSizes.get();

        if (!dataSizesMap.containsKey(nodeID))
        {
            initializeEmptyNodeMap(nodeID);
        }

        ImmutableMap<TableReference, Long> dataSizes = dataSizesMap.get(nodeID);

        if (dataSizes != null && !dataSizes.isEmpty())
        {
            return dataSizes.values().stream().mapToLong(e -> e).sum();
        }

        return 0;
    }

    @Override
    public void close()
    {
        myScheduledExecutorService.shutdown();

        myTableSizes.set(null);
    }

    /**
     * Creates a new builder for constructing {@link TableStorageStatesImpl} instances.
     *
     * @return a new {@link Builder} instance.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder class for constructing {@link TableStorageStatesImpl} instances.
     */
    public static class Builder
    {
        private ReplicatedTableProvider myReplicatedTableProvider;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private DistributedNativeConnectionProvider myNativeConnectionProvider;

        private long myInitialDelayInMs = 0;
        private long myUpdateDelayInMs = DEFAULT_UPDATE_DELAY_IN_MS;

        /**
         * Sets the replicated table provider.
         *
         * @param replicatedTableProvider the provider for replicated tables.
         * @return this builder instance.
         */
        public final Builder withReplicatedTableProvider(final ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        /**
         * Sets the JMX proxy factory.
         *
         * @param jmxProxyFactory the factory for creating JMX proxy connections.
         * @return this builder instance.
         */
        public final Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Sets the native connection provider.
         *
         * @param nativeConnectionProvider the native connection provider for accessing Cassandra nodes.
         * @return this builder instance.
         */
        public final Builder withConnectionProvider(final DistributedNativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnectionProvider = nativeConnectionProvider;
            return this;
        }

        /**
         * Sets the initial delay before the first table state update.
         *
         * @param initialDelay the initial delay value.
         * @param timeUnit the time unit of the delay.
         * @return this builder instance.
         */
        public final Builder withInitialDelay(final long initialDelay, final TimeUnit timeUnit)
        {
            myInitialDelayInMs = timeUnit.toMillis(initialDelay);
            return this;
        }

        /**
         * Sets the delay between consecutive table state updates.
         *
         * @param updateDelay the update delay value.
         * @param timeUnit the time unit of the delay.
         * @return this builder instance.
         */
        public final Builder withUpdateDelay(final long updateDelay, final TimeUnit timeUnit)
        {
            myUpdateDelayInMs = timeUnit.toMillis(updateDelay);
            return this;
        }

        /**
         * Builds a new {@link TableStorageStatesImpl} instance using the configured parameters.
         *
         * @return a new TableStorageStatesImpl instance.
         * @throws IllegalArgumentException if any required parameter is null.
         */
        public final TableStorageStatesImpl build()
        {
            if (myReplicatedTableProvider == null)
            {
                throw new IllegalArgumentException("Replicated table provider cannot be null");
            }

            if (myJmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX proxy factory cannot be null");
            }

            if (myNativeConnectionProvider == null)
            {
                throw new IllegalArgumentException("Native connection provider cannot be null");
            }

            return new TableStorageStatesImpl(this);
        }
    }

    @VisibleForTesting
    void updateTableStates()
    {
        if (myJmxProxyFactory != null)
        {
            for (UUID nodeID : myTableSizes.get().keySet())
            {
                try (DistributedJmxProxy jmxProxy = myJmxProxyFactory.connect())
                {
                    Map<UUID, ImmutableMap<TableReference, Long>> newEntry = new HashMap<>();
                    ImmutableMap<TableReference, Long> tableSize = getTableSizes(nodeID, jmxProxy);
                    newEntry.put(nodeID, tableSize);

                    myTableSizes.set(newEntry);
                }
                catch (IOException e)
                {
                    LOG.error("Unable to update table sizes, future metrics might contain stale data", e);
                }
            }
        }
    }

    private ImmutableMap<TableReference, Long> getTableSizes(final UUID nodeID, final DistributedJmxProxy jmxProxy)
    {
        Map<TableReference, Long> dataSizes = new HashMap<>();

        if (myReplicatedTableProvider != null)
        {
            for (TableReference tableReference : myReplicatedTableProvider.getAll())
            {
                long diskSpaceUsed = jmxProxy.liveDiskSpaceUsed(nodeID, tableReference);

                LOG.debug("{} -> {}", tableReference, diskSpaceUsed);
                dataSizes.put(tableReference, diskSpaceUsed);
            }
        }

        return ImmutableMap.copyOf(dataSizes);
    }
}

