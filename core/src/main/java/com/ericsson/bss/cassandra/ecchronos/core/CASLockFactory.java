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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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

    private final UUID myUuid;
    private final HostStates myHostStates;
    private final CASLockFactoryCacheContext myCasLockFactoryCacheContext;

    private final CASLockProperties myCasLockProperties;
    private final CASLockStatement myCasLockStatement;

    CASLockFactory(final CASLockFactoryBuilder builder)
    {
        myCasLockProperties = new CASLockProperties(
            builder.getNativeConnectionProvider().getRemoteRouting(),
            builder.getKeyspaceName(),
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("LockRefresher-%d").build()),
            builder.getConsistencyType(),
            builder.getNativeConnectionProvider().getSession(),
            builder.getStatementDecorator());

        myHostStates = builder.getHostStates();

        verifySchemasExists();

        UUID hostId = builder.getNativeConnectionProvider().getLocalNode().getHostId();

        if (hostId == null)
        {
            hostId = UUID.randomUUID();
            LOG.warn("Unable to determine local nodes host id, using {} instead", hostId);
        }

        myUuid = hostId;
        myCasLockFactoryCacheContext = buildCasLockFactoryCacheContext(builder.getCacheExpiryTimeInSecond());

        myCasLockStatement = new CASLockStatement(myCasLockProperties, myCasLockFactoryCacheContext);
    }

    private CASLockFactoryCacheContext buildCasLockFactoryCacheContext(final long cacheExpiryTimeInSeconds)
    {
        int lockTimeInSeconds = getDefaultTimeToLiveFromLockTable();
        int lockUpdateTimeInSeconds = lockTimeInSeconds / REFRESH_INTERVAL_RATIO;
        int myFailedLockRetryAttempts = (lockTimeInSeconds / lockUpdateTimeInSeconds) - 1;

        return CASLockFactoryCacheContext.newBuilder()
                .withLockUpdateTimeInSeconds(lockUpdateTimeInSeconds)
                .withFailedLockRetryAttempts(myFailedLockRetryAttempts)
                .withLockCache(new LockCache(this::doTryLock, cacheExpiryTimeInSeconds))
                .build();
    }

    private int getDefaultTimeToLiveFromLockTable()
    {
        TableMetadata tableMetadata = myCasLockProperties.getSession().getMetadata()
                .getKeyspace(myCasLockProperties.getKeyspaceName())
                .flatMap(ks -> ks.getTable(TABLE_LOCK))
                .orElse(null);
        if (tableMetadata == null || tableMetadata.getOptions() == null)
        {
            LOG.warn("Could not parse default ttl of {}.{}", myCasLockProperties.getKeyspaceName(), TABLE_LOCK);
            return DEFAULT_LOCK_TIME_IN_SECONDS;
        }
        Map<CqlIdentifier, Object> tableOptions = tableMetadata.getOptions();
        return (Integer) tableOptions.get(CqlIdentifier.fromInternal("default_time_to_live"));
    }

    @Override
    public DistributedLock tryLock(final String dataCenter,
                                   final String resource,
                                   final int priority,
                                   final Map<String, String> metadata)
                                                                       throws LockException
    {
        return myCasLockFactoryCacheContext.getLockCache()
                .getLock(dataCenter, resource, priority, metadata);
    }

    @Override
    public Map<String, String> getLockMetadata(final String dataCenter, final String resource) throws LockException
    {
        ResultSet resultSet = myCasLockStatement.execute(
            dataCenter, myCasLockStatement.getLockMetadataStatement().bind(resource));

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
    public boolean sufficientNodesForLocking(final String dataCenter, final String resource)
    {
        try
        {
            Set<Node> nodes = getNodesForResource(dataCenter, resource);

            int quorum = nodes.size() / 2 + 1;
            int liveNodes = liveNodes(nodes);

            LOG.trace("Live nodes {}, quorum: {}", liveNodes, quorum);

            return liveNodes >= quorum;
        }
        catch (UnsupportedEncodingException e)
        {
            LOG.warn("Unable to encode resource bytes", e);
        }

        return false;
    }

    @Override
    public Optional<LockException> getCachedFailure(final String dataCenter, final String resource)
    {
        return myCasLockFactoryCacheContext.getLockCache().getCachedFailure(dataCenter, resource);
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
    UUID getHostId()
    {
        return myUuid;
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
                                      final Map<String, String> metadata) throws LockException
    {
        LOG.trace("Trying lock for {} - {}", dataCenter, resource);

        if (!sufficientNodesForLocking(dataCenter, resource))
        {
            LOG.warn("Not sufficient nodes to lock resource {} in datacenter {}", resource, dataCenter);
            throw new LockException("Not sufficient nodes to lock");
        }
        CASLock casLock = new CASLock(dataCenter, resource, priority, metadata, myUuid, myCasLockStatement); // NOSONAR
        if (casLock.lock())
        {
            return casLock;
        }
        else
        {
            throw new LockException(String.format("Unable to lock resource %s in datacenter %s", resource, dataCenter));
        }
    }

    private Set<Node> getNodesForResource(final String dataCenter,
                                          final String resource) throws UnsupportedEncodingException
    {
        Set<Node> dataCenterNodes = new HashSet<>();

        Metadata metadata = myCasLockProperties.getSession().getMetadata();
        TokenMap tokenMap = metadata.getTokenMap()
                .orElseThrow(() -> new IllegalStateException("Couldn't get token map, is it disabled?"));
        Set<Node> nodes = tokenMap.getReplicas(
                myCasLockProperties.getKeyspaceName(), ByteBuffer.wrap(resource.getBytes("UTF-8")));

        if (dataCenter != null)
        {
            Iterator<Node> iterator = nodes.iterator();

            while (iterator.hasNext())
            {
                Node node = iterator.next();

                if (dataCenter.equals(node.getDatacenter()))
                {
                    dataCenterNodes.add(node);
                }
            }

            return dataCenterNodes;
        }

        return nodes;
    }

    private int liveNodes(final Collection<Node> nodes)
    {
        int live = 0;
        for (Node node : nodes)
        {
            if (myHostStates.isUp(node))
            {
                live++;
            }
        }
        return live;
    }

    private void verifySchemasExists()
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = myCasLockProperties
            .getSession().getMetadata()
                .getKeyspace(myCasLockProperties.getKeyspaceName());
        if (!keyspaceMetadata.isPresent())
        {
            LOG.error("Keyspace {} does not exist, it needs to be created",
                myCasLockProperties.getKeyspaceName());
            throw new IllegalStateException(
                    String.format("Keyspace %s does not exist, it needs to be created",
                        myCasLockProperties.getKeyspaceName()));
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK).isPresent())
        {
            LOG.error("Table {}.{} does not exist, it needs to be created",
                myCasLockProperties.getKeyspaceName(), TABLE_LOCK);
            throw new IllegalStateException(
                    String.format("Table %s.%s does not exist, it needs to be created",
                        myCasLockProperties.getKeyspaceName(), TABLE_LOCK));
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK_PRIORITY).isPresent())
        {
            LOG.error("Table {}.{} does not exist, it needs to be created",
                        myCasLockProperties.getKeyspaceName(), TABLE_LOCK_PRIORITY);
            throw new IllegalStateException(
                    String.format("Table %s.%s does not exist, it needs to be created",
                        myCasLockProperties.getKeyspaceName(), TABLE_LOCK_PRIORITY));
        }
    }

}
