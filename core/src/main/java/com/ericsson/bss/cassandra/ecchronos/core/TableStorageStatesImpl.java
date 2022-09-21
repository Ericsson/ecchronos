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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

public final class TableStorageStatesImpl implements TableStorageStates, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TableStorageStatesImpl.class);

    private static final long DEFAULT_UPDATE_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(60);

    private final AtomicReference<ImmutableMap<TableReference, Long>> myTableSizes = new AtomicReference<>();
    private final ScheduledExecutorService myScheduledExecutorService;

    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final JmxProxyFactory myJmxProxyFactory;

    private TableStorageStatesImpl(final Builder builder)
    {
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myJmxProxyFactory = builder.myJmxProxyFactory;

        myScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        myScheduledExecutorService.scheduleAtFixedRate(this::updateTableStates,
                builder.myInitialDelayInMs,
                builder.myUpdateDelayInMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public long getDataSize(final TableReference tableReference)
    {
        ImmutableMap<TableReference, Long> dataSizes = myTableSizes.get();

        if (dataSizes != null && dataSizes.containsKey(tableReference))
        {
            return dataSizes.get(tableReference);
        }

        return 0;
    }

    @Override
    public long getDataSize()
    {
        ImmutableMap<TableReference, Long> dataSizes = myTableSizes.get();

        if (dataSizes != null)
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ReplicatedTableProvider myReplicatedTableProvider;
        private JmxProxyFactory myJmxProxyFactory;

        private long myInitialDelayInMs = 0;
        private long myUpdateDelayInMs = DEFAULT_UPDATE_DELAY_IN_MS;

        public final Builder withReplicatedTableProvider(final ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        public final Builder withJmxProxyFactory(final JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public final Builder withInitialDelay(final long initialDelay, final TimeUnit timeUnit)
        {
            myInitialDelayInMs = timeUnit.toMillis(initialDelay);
            return this;
        }

        public final Builder withUpdateDelay(final long updateDelay, final TimeUnit timeUnit)
        {
            myUpdateDelayInMs = timeUnit.toMillis(updateDelay);
            return this;
        }

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

            return new TableStorageStatesImpl(this);
        }
    }

    @VisibleForTesting
    void updateTableStates()
    {
        if (myJmxProxyFactory != null)
        {
            try (JmxProxy jmxProxy = myJmxProxyFactory.connect())
            {
                myTableSizes.set(getTableSizes(jmxProxy));
            }
            catch (IOException e)
            {
                LOG.error("Unable to update table sizes, future metrics might contain stale data", e);
            }
        }
    }

    private ImmutableMap<TableReference, Long> getTableSizes(final JmxProxy jmxProxy)
    {
        Map<TableReference, Long> dataSizes = new HashMap<>();

        if (myReplicatedTableProvider != null)
        {
            for (TableReference tableReference : myReplicatedTableProvider.getAll())
            {
                long diskSpaceUsed = jmxProxy.liveDiskSpaceUsed(tableReference);

                LOG.debug("{} -> {}", tableReference, diskSpaceUsed);
                dataSizes.put(tableReference, diskSpaceUsed);
            }
        }

        return ImmutableMap.copyOf(dataSizes);
    }
}
