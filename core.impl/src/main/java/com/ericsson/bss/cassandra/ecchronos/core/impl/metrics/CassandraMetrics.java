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
package com.ericsson.bss.cassandra.ecchronos.core.impl.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.impl.logging.ThrottlingLogger;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Used to fetch metrics from Cassandra through JMX and keep them updated.
 */
public class CassandraMetrics implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraMetrics.class);
    private static final ThrottlingLogger THROTTLED_LOGGER = new ThrottlingLogger(LOG, 5, TimeUnit.MINUTES);
    private static final long DEFAULT_CACHE_EXPIRY_TIME_IN_MINUTES = 60;
    private static final long DEFAULT_CACHE_REFRESH_TIME_IN_SECONDS = 30;

    private final LoadingCache<MetricsKey, CassandraMetric> myCache;
    private final DistributedJmxProxyFactory myJmxProxyFactory;

    /**
     * Constructs a CassandraMetrics instance with default cache refresh and expiry times.
     *
     * @param jmxProxyFactory the factory used to create connections to distributed JMX proxies. Must not be {@code null}.
     */
    public CassandraMetrics(final DistributedJmxProxyFactory jmxProxyFactory)
    {
        this(jmxProxyFactory, Duration.ofSeconds(DEFAULT_CACHE_REFRESH_TIME_IN_SECONDS),
                Duration.ofMinutes(DEFAULT_CACHE_EXPIRY_TIME_IN_MINUTES));
    }

    /**
     * Constructs a CassandraMetrics instance.
     *
     * @param jmxProxyFactory the factory used to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param refreshAfter the duration after which the cache will refresh its entries. Must not be {@code null}.
     * @param expireAfter the duration after which the cache entries will expire after access. Must not be {@code null}.
     */
    public CassandraMetrics(final DistributedJmxProxyFactory jmxProxyFactory, final Duration refreshAfter,
            final Duration expireAfter)
    {
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myCache = Caffeine.newBuilder()
                .refreshAfterWrite(Preconditions.checkNotNull(refreshAfter, "Refresh after must be set"))
                .expireAfterAccess(Preconditions.checkNotNull(expireAfter, "Expire after must be set"))
                .executor(Runnable::run)
                .build(this::getMetrics);
    }

    private CassandraMetric getMetrics(final MetricsKey key) throws IOException
    {
        try (DistributedJmxProxy jmxProxy = myJmxProxyFactory.connect())
        {
            long maxRepairedAt = jmxProxy.getMaxRepairedAt(key.nodeId(), key.tableReference());
            double percentRepaired = jmxProxy.getPercentRepaired(key.nodeId(), key.tableReference());
            LOG.trace("{}, maxRepairedAt: {}, percentRepaired: {}", key.tableReference(), maxRepairedAt, percentRepaired);
            return new CassandraMetric(percentRepaired, maxRepairedAt);
        }
        catch (IOException e)
        {
            THROTTLED_LOGGER.warn("Unable to fetch metrics from Cassandra, future metrics might contain stale values",
                    e);
            throw e;
        }
    }

    @VisibleForTesting
    final void refreshCache(final UUID nodeID, final TableReference tableReference)
    {
        MetricsKey key = new MetricsKey(nodeID, tableReference);
        myCache.refresh(key);
    }

    /**
     * Return max repaired at for a table.
     * @param nodeID the node ID
     * @param tableReference The table
     * @return Timestamp or 0 if not available.
     */
    public long getMaxRepairedAt(final UUID nodeID, final TableReference tableReference)
    {
        try
        {
            MetricsKey key = new MetricsKey(nodeID, tableReference);
            CassandraMetric cassandraMetric = myCache.get(key);
            return cassandraMetric.myMaxRepairedAt;
        }
        catch (CompletionException e)
        {
            THROTTLED_LOGGER.error("Failed to fetch maxRepairedAt metric for {}", tableReference, e);
            return 0L;
        }
    }

    /**
     * Return percent repaired for a table.
     * @param nodeID the node ID
     * @param tableReference The table
     * @return Percent repaired or 0 if not available.
     */
    public double getPercentRepaired(final UUID nodeID, final TableReference tableReference)
    {
        try
        {
            MetricsKey key = new MetricsKey(nodeID, tableReference);
            CassandraMetric cassandraMetric = myCache.get(key);
            return cassandraMetric.myPercentRepaired;
        }
        catch (CompletionException e)
        {
            THROTTLED_LOGGER.error("Failed to fetch percentRepaired metric for {}", tableReference, e);
            return 0.0d;
        }
    }

    /**
     * Cleans the cache.
     */
    @Override
    public void close()
    {
        myCache.invalidateAll();
        myCache.cleanUp();
    }

    private static class CassandraMetric
    {
        private final double myPercentRepaired;
        private final long myMaxRepairedAt;

        CassandraMetric(final Double percentRepaired, final Long maxRepairedAt)
        {
            myPercentRepaired = percentRepaired;
            myMaxRepairedAt = maxRepairedAt;
        }
    }

    private record MetricsKey(UUID nodeId, TableReference tableReference)
    {
    }

}

