/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraMetrics implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraMetrics.class);
    private static final long DEFAULT_INITIAL_DELAY_IN_MS = 0;
    private static final long DEFAULT_UPDATE_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(60);

    private final AtomicReference<ImmutableMap<TableReference, Double>> myPercentRepaired = new AtomicReference<>();
    private final AtomicReference<ImmutableMap<TableReference, Long>> myMaxRepairedAt = new AtomicReference<>();
    private final ScheduledExecutorService myScheduledExecutorService;
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final JmxProxyFactory myJmxProxyFactory;

    CassandraMetrics(final Builder builder)
    {
        myReplicatedTableProvider = Preconditions.checkNotNull(builder.myReplicatedTableProvider,
                "Replicated table provider must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(builder.myJmxProxyFactory,
                "JMX proxy factory must be set");
        myScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("CassandraMetricsUpdater-%d").build());
        myScheduledExecutorService.scheduleAtFixedRate(this::updateMetrics,
                builder.myInitialDelayInMs,
                builder.myUpdateDelayInMs,
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    final void updateMetrics()
    {
        try (JmxProxy jmxProxy = myJmxProxyFactory.connect())
        {
            Map<TableReference, Double> tablesPercentRepaired = new HashMap<>();
            Map<TableReference, Long> tablesMaxRepairedAt = new HashMap<>();
            for (TableReference tableReference : myReplicatedTableProvider.getAll())
            {
                long maxRepairedAt = jmxProxy.getMaxRepairedAt(tableReference);
                tablesMaxRepairedAt.put(tableReference, maxRepairedAt);

                double percentRepaired = jmxProxy.getPercentRepaired(tableReference);
                tablesPercentRepaired.put(tableReference, percentRepaired);
                LOG.debug("{}, maxRepairedAt: {}, percentRepaired: {}", tableReference, maxRepairedAt, percentRepaired);
            }
            myPercentRepaired.set(ImmutableMap.copyOf(tablesPercentRepaired));
            myMaxRepairedAt.set(ImmutableMap.copyOf(tablesMaxRepairedAt));
        }
        catch (IOException e)
        {
            LOG.error("Unable to update Cassandra metrics, future metrics might contain stale data", e);
        }
    }

    /**
     * Return max repaired at for a table.
     * @param tableReference The table
     * @return Timestamp or 0 if not available.
     */
    public long getMaxRepairedAt(final TableReference tableReference)
    {
        ImmutableMap<TableReference, Long> maxRepairedAt = myMaxRepairedAt.get();
        if (maxRepairedAt != null && maxRepairedAt.containsKey(tableReference))
        {
            return maxRepairedAt.get(tableReference);
        }
        return 0L;
    }

    /**
     * Return percent repaired for a table.
     * @param tableReference The table
     * @return Percent repaired or 0 if not available.
     */
    public double getPercentRepaired(final TableReference tableReference)
    {
        ImmutableMap<TableReference, Double> percentRepaired = myPercentRepaired.get();
        if (percentRepaired != null && percentRepaired.containsKey(tableReference))
        {
            return percentRepaired.get(tableReference);
        }
        return 0.0d;
    }

    @Override
    public final void close()
    {
        myScheduledExecutorService.shutdown();
        myPercentRepaired.set(null);
        myMaxRepairedAt.set(null);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ReplicatedTableProvider myReplicatedTableProvider;
        private JmxProxyFactory myJmxProxyFactory;
        private long myInitialDelayInMs = DEFAULT_INITIAL_DELAY_IN_MS;
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

        public final CassandraMetrics build()
        {
            return new CassandraMetrics(this);
        }
    }
}
