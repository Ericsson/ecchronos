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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Lock factory using Cassandras LWT (Compare-And-Set operations) to create and maintain locks.
 *
 * Expected keyspace/tables:
 * <pre>
 * CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
 *
 * CREATE TABLE IF NOT EXISTS ecchronos.lock (
 * resource text,
 * node uuid,
 * metadata map&lt;text,text&gt;,
 * PRIMARY KEY(resource))
 * WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
 *
 * CREATE TABLE IF NOT EXISTS ecchronos.lock_priority(
 * resource text,
 * node uuid,
 * priority int,
 * PRIMARY KEY(resource, node))
 * WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
 * </pre>
 */
public final class CASLockFactory implements LockFactory, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CASLockFactory.class);

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";
    private static final int REFRESH_INTERVAL_RATIO = 10;
    private static final int DEFAULT_LOCK_TIME_IN_SECONDS = 600;
    private static final int MINIMUM_LOCK_TIME_IN_SECONDS = 30;
    private static final long PRIORITY_CACHE_EXPIRE_SECONDS = 30L;
    private static final int LOCK_REFRESHER_POOL_SIZE = 2;

    private final CASLockFactoryCacheContext myCasLockFactoryCacheContext;
    private final Cache<String, List<NodePriority>> myPriorityCache;

    private final CASLockProperties myCasLockProperties;
    private final CASLockStatement myCasLockStatement;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final long myCacheExpiryTimeInSeconds;

    CASLockFactory(final CASLockFactoryBuilder builder)
    {
        myCasLockProperties = new CASLockProperties(
                builder.getNativeConnectionProvider().getConnectionType(),
                builder.getKeyspaceName(),
                Executors.newScheduledThreadPool(
                        LOCK_REFRESHER_POOL_SIZE,
                        new ThreadFactoryBuilder().setNameFormat("LockRefresher-%d").build()),
                builder.getConsistencyType(),
                builder.getNativeConnectionProvider().getCqlSession());

        myNativeConnectionProvider = builder.getNativeConnectionProvider();

        verifySchemasExists();
        myCacheExpiryTimeInSeconds = builder.getCacheExpiryTimeInSecond();

        myPriorityCache = Caffeine.newBuilder()
                .expireAfterWrite(PRIORITY_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
                .executor(Runnable::run)
                .build();

        myCasLockFactoryCacheContext = buildCasLockFactoryCacheContext();

        myCasLockStatement = new CASLockStatement(myCasLockProperties, myCasLockFactoryCacheContext);
    }

    private CASLockFactoryCacheContext buildCasLockFactoryCacheContext()
    {
        int lockTimeInSeconds = getDefaultTimeToLiveFromLockTable();
        int lockUpdateTimeInSeconds = lockTimeInSeconds / REFRESH_INTERVAL_RATIO;
        int myFailedLockRetryAttempts = (lockTimeInSeconds / lockUpdateTimeInSeconds) - 1;
        Map<UUID, LockCache> lockCache = new ConcurrentHashMap<>();
        myNativeConnectionProvider.getNodes().keySet().stream().forEach(uuid ->
        {
            LockCache.LockSupplier nodeSpecificSupplier = (dataCenter, resource, priority, metadata) ->
                doTryLock(dataCenter, resource, priority, metadata, uuid);
            lockCache.put(uuid, new LockCache(nodeSpecificSupplier, myCacheExpiryTimeInSeconds));
        });

        return CASLockFactoryCacheContext.newBuilder()
                .withLockUpdateTimeInSeconds(lockUpdateTimeInSeconds)
                .withFailedLockRetryAttempts(myFailedLockRetryAttempts)
                .withLockCache(lockCache)
                .build();
    }

    public void addLockCache(final UUID nodeId)
    {
        LockCache.LockSupplier nodeSpecificSupplier = (dataCenter, resource, priority, metadata) ->
                doTryLock(dataCenter, resource, priority, metadata, nodeId);
        myCasLockFactoryCacheContext.addLockCache(nodeId, new LockCache(nodeSpecificSupplier, myCacheExpiryTimeInSeconds));
    }

    private int getDefaultTimeToLiveFromLockTable()
    {
        TableMetadata tableMetadata = myCasLockProperties.getSession()
                .getMetadata()
                .getKeyspace(myCasLockProperties.getKeyspaceName())
                .flatMap(ks -> ks.getTable(TABLE_LOCK))
                .orElse(null);
        if (tableMetadata == null || tableMetadata.getOptions() == null)
        {
            LOG.warn("Could not parse default ttl of {}.{}, using default {}s",
                    myCasLockProperties.getKeyspaceName(), TABLE_LOCK, DEFAULT_LOCK_TIME_IN_SECONDS);
            return DEFAULT_LOCK_TIME_IN_SECONDS;
        }
        Map<CqlIdentifier, Object> tableOptions = tableMetadata.getOptions();
        Object ttlValue = tableOptions.get(CqlIdentifier.fromInternal("default_time_to_live"));
        if (ttlValue == null)
        {
            LOG.warn("No default_time_to_live found for {}.{}, using default {}s",
                    myCasLockProperties.getKeyspaceName(), TABLE_LOCK, DEFAULT_LOCK_TIME_IN_SECONDS);
            return DEFAULT_LOCK_TIME_IN_SECONDS;
        }
        int ttl = (Integer) ttlValue;
        if (ttl < MINIMUM_LOCK_TIME_IN_SECONDS)
        {
            LOG.warn("default_time_to_live for {}.{} is {}s, using minimum {}s",
                    myCasLockProperties.getKeyspaceName(), TABLE_LOCK, ttl, MINIMUM_LOCK_TIME_IN_SECONDS);
            return MINIMUM_LOCK_TIME_IN_SECONDS;
        }
        return ttl;
    }

    @Override
    public DistributedLock tryLock(final String dataCenter,
                                               final String resource,
                                               final int priority,
                                               final Map<String, String> metadata,
                                               final UUID nodeId) throws LockException
    {
        verifyNodeOnLockCacheMap(nodeId);
        return myCasLockFactoryCacheContext.getLockCache(nodeId)
                .getLock(dataCenter, resource, priority, metadata);
    }

    private void verifyNodeOnLockCacheMap(final UUID nodeId) throws LockException
    {
        if (myCasLockFactoryCacheContext.getLockCache(nodeId) == null)
        {
            if (myNativeConnectionProvider.getNodes().keySet().contains(nodeId))
            {
                addLockCache(nodeId);
            }
            else
            {
                throw new LockException("Node " + nodeId + " is not managed by local instance");
            }
        }
        else if (!myNativeConnectionProvider.getNodes().keySet().contains(nodeId))
        {
            myCasLockFactoryCacheContext.removeLockCache(nodeId);
            throw new LockException("Node " + nodeId + " is no longer managed by local instance");
        }
    }

    @Override
    public Map<String, String> getLockMetadata(final String resource) throws LockException
    {
        ResultSet resultSet = myCasLockStatement.execute(
                myCasLockStatement.getLockMetadataStatement().bind(resource));

        Row row = resultSet.one();

        if (row != null)
        {
            return row.getMap("metadata", String.class, String.class);
        }
        else
        {
            throw new LockException("Unable to retrieve metadata for resource " + resource);
        }
    }



    @Override
    public Optional<LockException> getCachedFailure(final UUID nodeID, final String dataCenter, final String resource)
    {
        try
        {
            verifyNodeOnLockCacheMap(nodeID);
        }
        catch (LockException e)
        {
            LOG.debug("Unable to verify node on lock cache map for node {}", nodeID, e);
        }
        return myCasLockFactoryCacheContext.getLockCache(nodeID).getCachedFailure(dataCenter, resource);
    }

    @Override
    public void close()
    {
        myCasLockProperties.getExecutor().shutdown();
        try
        {
            if (!myCasLockProperties.getExecutor().awaitTermination(1, TimeUnit.SECONDS))
            {
                LOG.warn("Executing tasks did not finish within one second");
            }
        }
        catch (InterruptedException e)
        {
            LOG.warn("Interrupted while waiting for executor to shut down", e);
        }
    }

    @VisibleForTesting
    CASLockFactoryCacheContext getCasLockFactoryCacheContext()
    {
        return myCasLockFactoryCacheContext;
    }

    @VisibleForTesting
    CASLockStatement getCasLockStatement()
    {
        return myCasLockStatement;
    }

    @VisibleForTesting
    ConsistencyLevel getSerialConsistencyLevel()
    {
        return myCasLockProperties.getSerialConsistencyLevel();
    }

    public static CASLockFactoryBuilder builder()
    {
        return new CASLockFactoryBuilder();
    }

    private DistributedLock doTryLock(final String dataCenter,
                                      final String resource,
                                      final int priority,
                                      final Map<String, String> metadata,
                                      final UUID nodeId) throws LockException
    {
        LOG.trace("Trying lock for {} - {}", dataCenter, resource);

        try
        {
            List<NodePriority> priorities = getPrioritiesForResource(resource);
            CASLock casLock = new CASLock(resource, priority, metadata, nodeId, myCasLockStatement, priorities); // NOSONAR
            if (casLock.lock())
            {
                return casLock;
            }
            else
            {
                throw new LockException(String.format("Unable to lock resource %s in datacenter %s", resource, dataCenter));
            }
        }
        catch (DriverException e)
        {
            LOG.warn("Unable to lock resource {}, not enough nodes available", resource, e);
            throw new LockException("Not enough nodes available to lock resource " + resource, e);
        }
    }

    List<NodePriority> getPrioritiesForResource(final String resource)
    {
        return myPriorityCache.get(resource, this::fetchPriorities);
    }

    private List<NodePriority> fetchPriorities(final String resource)
    {
        List<NodePriority> nodePriorities = new ArrayList<>();
        ResultSet resultSet = myCasLockStatement.execute(
                myCasLockStatement.getGetPriorityStatement().bind(resource));
        for (Row row : resultSet)
        {
            nodePriorities.add(new NodePriority(
                    row.getUuid(CASLockStatement.COLUMN_NODE),
                    row.getInt(CASLockStatement.COLUMN_PRIORITY)));
        }
        return nodePriorities;
    }





    private void verifySchemasExists()
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = myCasLockProperties
                .getSession()
                .getMetadata()
                .getKeyspace(myCasLockProperties.getKeyspaceName());

        if (!keyspaceMetadata.isPresent())
        {
            String msg = String.format("Keyspace %s does not exist, it needs to be created",
                    myCasLockProperties.getKeyspaceName());
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK).isPresent())
        {
            String msg = String.format("Table %s.%s does not exist, it needs to be created",
                    myCasLockProperties.getKeyspaceName(),
                    TABLE_LOCK);
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK_PRIORITY).isPresent())
        {
            String msg = String.format("Table %s.%s does not exist, it needs to be created",
                    myCasLockProperties.getKeyspaceName(),
                    TABLE_LOCK_PRIORITY);
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

}
