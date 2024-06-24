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

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.logging.ThrottlingLogger;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
    private static final long DEFAULT_CACHE_EXPIRY_TIME_MINUTES = 60;
    private static final long DEFAULT_CACHE_REFRESH_TIME_SECONDS = 30;

    private final LoadingCache<TableReference, CassandraMetric> myCache;
    private final JmxProxyFactory myJmxProxyFactory;

    public CassandraMetrics(final JmxProxyFactory jmxProxyFactory)
    {
        this(jmxProxyFactory, Duration.ofSeconds(DEFAULT_CACHE_REFRESH_TIME_SECONDS),
                Duration.ofMinutes(DEFAULT_CACHE_EXPIRY_TIME_MINUTES));
    }
    public CassandraMetrics(final JmxProxyFactory jmxProxyFactory, final Duration refreshAfter,
            final Duration expireAfter)
    {
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myCache = Caffeine.newBuilder()
                .refreshAfterWrite(Preconditions.checkNotNull(refreshAfter, "Refresh after must be set"))
                .expireAfterAccess(Preconditions.checkNotNull(expireAfter, "Expire after must be set"))
                .executor(Runnable::run)
                .build(this::getMetrics);
    }

    private CassandraMetric getMetrics(final TableReference tableReference) throws IOException
    {
        try (JmxProxy jmxProxy = myJmxProxyFactory.connect())
        {
            long maxRepairedAt = jmxProxy.getMaxRepairedAt(tableReference);
            double percentRepaired = jmxProxy.getPercentRepaired(tableReference);
            LOG.debug("{}, maxRepairedAt: {}, percentRepaired: {}", tableReference, maxRepairedAt, percentRepaired);
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
    final void refreshCache(final TableReference tableReference)
    {
        myCache.refresh(tableReference);
    }

    /**
     * Return max repaired at for a table.
     * @param tableReference The table
     * @return Timestamp or 0 if not available.
     */
    public long getMaxRepairedAt(final TableReference tableReference)
    {
        try
        {
            CassandraMetric cassandraMetric = myCache.get(tableReference);
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
     * @param tableReference The table
     * @return Percent repaired or 0 if not available.
     */
    public double getPercentRepaired(final TableReference tableReference)
    {
        try
        {
            CassandraMetric cassandraMetric = myCache.get(tableReference);
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

    private class CassandraMetric
    {
        private final double myPercentRepaired;
        private final long myMaxRepairedAt;

        CassandraMetric(final Double percentRepaired, final Long maxRepairedAt)
        {
            myPercentRepaired = percentRepaired;
            myMaxRepairedAt = maxRepairedAt;
        }
    }
}
