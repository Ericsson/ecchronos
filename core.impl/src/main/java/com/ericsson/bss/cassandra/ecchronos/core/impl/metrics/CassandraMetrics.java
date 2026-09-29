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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Used to fetch metrics from Cassandra through JMX and keep them updated.
 * <p>
 * Reads are served from a Caffeine cache. Both the automatic {@code refreshAfterWrite} refreshes and the JMX round
 * trip that backs them run off the calling thread and are time-bounded, so a stalled or unresponsive node cannot
 * block a caller (for example an HTTP request thread serving {@code GET /repair-management/schedules}) — the
 * last-known value is returned immediately and a failed fetch degrades to the {@code 0.0}/{@code 0L} fallback.
 */
public class CassandraMetrics implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraMetrics.class);
    private static final ThrottlingLogger THROTTLED_LOGGER = new ThrottlingLogger(LOG, 5, TimeUnit.MINUTES);
    private static final long DEFAULT_CACHE_EXPIRY_TIME_IN_MINUTES = 60;
    private static final long DEFAULT_CACHE_REFRESH_TIME_IN_SECONDS = 30;
    private static final long DEFAULT_CACHE_MAX_SIZE = 10_000;
    private static final long DEFAULT_JMX_FETCH_TIMEOUT_IN_SECONDS = 30;
    private static final int REFRESH_THREADS = 2;

    private final LoadingCache<MetricsKey, CassandraMetric> myCache;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final ExecutorService myRefreshExecutor;
    private final ExecutorService myFetchExecutor;
    private final long myFetchTimeoutMs;

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
     * Constructs a CassandraMetrics instance with the default JMX fetch timeout.
     *
     * @param jmxProxyFactory the factory used to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param refreshAfter the duration after which the cache will refresh its entries. Must not be {@code null}.
     * @param expireAfter the duration after which the cache entries will expire after access. Must not be {@code null}.
     */
    public CassandraMetrics(final DistributedJmxProxyFactory jmxProxyFactory, final Duration refreshAfter,
            final Duration expireAfter)
    {
        this(jmxProxyFactory, refreshAfter, expireAfter,
                Duration.ofSeconds(DEFAULT_JMX_FETCH_TIMEOUT_IN_SECONDS));
    }

    /**
     * Constructs a CassandraMetrics instance.
     *
     * @param jmxProxyFactory the factory used to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param refreshAfter the duration after which the cache will refresh its entries. Must not be {@code null}.
     * @param expireAfter the duration after which the cache entries will expire after access. Must not be {@code null}.
     * @param fetchTimeout the maximum time to wait for a single JMX metric fetch before giving up. Must not be
     *         {@code null}.
     */
    public CassandraMetrics(final DistributedJmxProxyFactory jmxProxyFactory, final Duration refreshAfter,
            final Duration expireAfter, final Duration fetchTimeout)
    {
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myFetchTimeoutMs = Preconditions.checkNotNull(fetchTimeout, "Fetch timeout must be set").toMillis();
        myRefreshExecutor = Executors.newScheduledThreadPool(REFRESH_THREADS,
                new ThreadFactoryBuilder().setNameFormat("CassandraMetricsRefresh-%d").setDaemon(true).build());
        myFetchExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("CassandraMetricsFetch-%d").setDaemon(true).build());
        myCache = Caffeine.newBuilder()
                .maximumSize(DEFAULT_CACHE_MAX_SIZE)
                .refreshAfterWrite(Preconditions.checkNotNull(refreshAfter, "Refresh after must be set"))
                .expireAfterAccess(Preconditions.checkNotNull(expireAfter, "Expire after must be set"))
                .executor(myRefreshExecutor)
                .build(this::getMetrics);
    }

    private CassandraMetric getMetrics(final MetricsKey key) throws IOException
    {
        // Bound the JMX round trip so a stalled/unresponsive node cannot block the caller (or a background
        // refresh worker) indefinitely. A timeout is treated like any other fetch failure.
        Future<CassandraMetric> future = myFetchExecutor.submit(fetchTask(key));
        try
        {
            return future.get(myFetchTimeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            future.cancel(true);
            THROTTLED_LOGGER.warn("Timed out fetching metrics from Cassandra for {} after {} ms, future metrics "
                    + "might contain stale values", key.tableReference(), myFetchTimeoutMs, e);
            throw new IOException("Timed out fetching metrics for " + key.tableReference(), e);
        }
        catch (ExecutionException e)
        {
            THROTTLED_LOGGER.warn("Unable to fetch metrics from Cassandra, future metrics might contain stale values",
                    e);
            throw new IOException("Unable to fetch metrics for " + key.tableReference(), e);
        }
        catch (InterruptedException e)
        {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching metrics for " + key.tableReference(), e);
        }
    }

    private Callable<CassandraMetric> fetchTask(final MetricsKey key)
    {
        return () ->
        {
            try (DistributedJmxProxy jmxProxy = myJmxProxyFactory.connect())
            {
                long maxRepairedAt = jmxProxy.getMaxRepairedAt(key.nodeId(), key.tableReference());
                double percentRepaired = jmxProxy.getPercentRepaired(key.nodeId(), key.tableReference());
                LOG.trace("{}, maxRepairedAt: {}, percentRepaired: {}", key.tableReference(), maxRepairedAt,
                        percentRepaired);
                return new CassandraMetric(percentRepaired, maxRepairedAt);
            }
        };
    }

    /**
     * Force a synchronous refresh of the cached metrics for the given node and table.
     * <p>
     * A plain read may return a value up to the {@code refreshAfterWrite} interval old (and automatic refreshes now
     * run in the background). Callers that need an up-to-date reading (for example to confirm that a just-completed
     * incremental repair actually advanced the repaired state) should call this first. This performs a
     * time-bounded fetch on the calling thread and stores the result, so the freshly loaded value is visible to
     * subsequent reads. If the fetch fails or times out the cached value is left unchanged.
     *
     * @param nodeID the node ID.
     * @param tableReference the table.
     */
    public void forceRefresh(final UUID nodeID, final TableReference tableReference)
    {
        MetricsKey key = new MetricsKey(nodeID, tableReference);
        try
        {
            myCache.put(key, getMetrics(key));
        }
        catch (IOException e)
        {
            THROTTLED_LOGGER.warn("Unable to force refresh metrics for {}, keeping previous value", tableReference, e);
        }
    }

    @VisibleForTesting
    final void refreshCache(final UUID nodeID, final TableReference tableReference)
    {
        forceRefresh(nodeID, tableReference);
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
     * Cleans the cache and shuts down the background executors.
     */
    @Override
    public void close()
    {
        myCache.invalidateAll();
        myCache.cleanUp();
        myRefreshExecutor.shutdownNow();
        myFetchExecutor.shutdownNow();
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

