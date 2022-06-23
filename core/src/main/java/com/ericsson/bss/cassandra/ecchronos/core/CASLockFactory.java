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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

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
//TODO REMOVE NOPMD
public class CASLockFactory implements LockFactory, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CASLockFactory.class);

    private static final String COLUMN_RESOURCE = "resource";
    private static final String COLUMN_NODE = "node";
    private static final String COLUMN_METADATA = "metadata";
    private static final String COLUMN_PRIORITY = "priority";

    private static final int LOCK_TIME_IN_SECONDS = 600;
    private static final long LOCK_UPDATE_TIME_IN_SECONDS = 60;
    private static final int FAILED_LOCK_RETRY_ATTEMPTS =
            (int) (LOCK_TIME_IN_SECONDS / LOCK_UPDATE_TIME_IN_SECONDS) - 1;

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private final UUID myUuid;

    private final ScheduledExecutorService myExecutor;

    private final StatementDecorator myStatementDecorator;
    private final HostStates myHostStates;
    private final boolean myRemoteRouting; // NOPMD

    private final CqlSession mySession;
    private final String myKeyspaceName;
    private final PreparedStatement myCompeteStatement;
    private final PreparedStatement myGetPriorityStatement;
    private final PreparedStatement myLockStatement;
    private final PreparedStatement myGetLockMetadataStatement;
    private final PreparedStatement myRemoveLockStatement;
    private final PreparedStatement myUpdateLockStatement;
    private final PreparedStatement myRemoveLockPriorityStatement;
    private final LockCache myLockCache;

    private CASLockFactory(Builder builder)
    {
        myStatementDecorator = builder.myStatementDecorator;
        myHostStates = builder.myHostStates;
        myKeyspaceName = builder.myKeyspaceName;

        myExecutor = Executors.newSingleThreadScheduledExecutor();

        mySession = builder.myNativeConnectionProvider.getSession();
        myRemoteRouting = builder.myNativeConnectionProvider.getRemoteRouting();

        verifySchemasExists();

        ConsistencyLevel serialConsistencyLevel = myRemoteRouting ?
                ConsistencyLevel.LOCAL_SERIAL :
                ConsistencyLevel.SERIAL;
        SimpleStatement insertLockStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_METADATA, bindMarker())
                .ifNotExists()
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(serialConsistencyLevel);

        SimpleStatement getLockMetadataStatement = QueryBuilder.selectFrom(myKeyspaceName, TABLE_LOCK)
                .column(COLUMN_METADATA)
                .whereColumn(COLUMN_RESOURCE).isEqualTo(bindMarker())
                .build()
                .setSerialConsistencyLevel(serialConsistencyLevel);

        SimpleStatement removeLockStatement = QueryBuilder.deleteFrom(myKeyspaceName, TABLE_LOCK)
                .whereColumn(COLUMN_RESOURCE).isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE).isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).setSerialConsistencyLevel(serialConsistencyLevel);

        SimpleStatement updateLockStatement = QueryBuilder.update(myKeyspaceName, TABLE_LOCK)
                .setColumn(COLUMN_NODE, bindMarker())
                .setColumn(COLUMN_METADATA, bindMarker())
                .whereColumn(COLUMN_RESOURCE).isEqualTo(bindMarker())
                .ifColumn(COLUMN_NODE).isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM).setSerialConsistencyLevel(serialConsistencyLevel);

        SimpleStatement competeStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_PRIORITY, bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        SimpleStatement getPriorityStatement = QueryBuilder.selectFrom(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .columns(COLUMN_PRIORITY, COLUMN_NODE)
                .whereColumn(COLUMN_RESOURCE).isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        SimpleStatement removeLockPriorityStatement = QueryBuilder.deleteFrom(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .whereColumn(COLUMN_RESOURCE).isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE).isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        myLockStatement = mySession.prepare(insertLockStatement);
        myGetLockMetadataStatement = mySession.prepare(getLockMetadataStatement);
        myRemoveLockStatement = mySession.prepare(removeLockStatement);
        myUpdateLockStatement = mySession.prepare(updateLockStatement);
        myCompeteStatement = mySession.prepare(competeStatement);
        myGetPriorityStatement = mySession.prepare(getPriorityStatement);
        myRemoveLockPriorityStatement = mySession.prepare(removeLockPriorityStatement);

        UUID hostId = builder.myNativeConnectionProvider.getLocalNode().getHostId();

        if (hostId == null)
        {
            hostId = UUID.randomUUID();
            LOG.warn("Unable to determine local nodes host id, using {} instead", hostId);
        }

        myUuid = hostId;

        myLockCache = new LockCache(this::doTryLock);
    }

    @Override
    public DistributedLock tryLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
            throws LockException
    {
        return myLockCache.getLock(dataCenter, resource, priority, metadata);
    }

    @Override
    public Map<String, String> getLockMetadata(String dataCenter, String resource)
    {
        try
        {
            ResultSet resultSet = execute(dataCenter, myGetLockMetadataStatement.bind(resource));

            Row row = resultSet.one();

            if (row != null)
            {
                return row.getMap("metadata", String.class, String.class);
            }
        }
        catch (Exception e)
        {
            LOG.warn("Unable to retrieve metadata for resource {}", resource, e);
        }

        return null;
    }

    @Override
    public boolean sufficientNodesForLocking(String dataCenter, String resource)
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
    public Optional<LockException> getCachedFailure(String dataCenter, String resource)
    {
        return myLockCache.getCachedFailure(dataCenter, resource);
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(1, TimeUnit.SECONDS))
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";

        private NativeConnectionProvider myNativeConnectionProvider;
        private HostStates myHostStates;
        private StatementDecorator myStatementDecorator;
        private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;

        public Builder withNativeConnectionProvider(NativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnectionProvider = nativeConnectionProvider;
            return this;
        }

        public Builder withHostStates(HostStates hostStates)
        {
            myHostStates = hostStates;
            return this;
        }

        public Builder withStatementDecorator(StatementDecorator statementDecorator)
        {
            myStatementDecorator = statementDecorator;
            return this;
        }

        public Builder withKeyspaceName(String keyspaceName)
        {
            myKeyspaceName = keyspaceName;
            return this;
        }

        public CASLockFactory build()
        {
            if (myNativeConnectionProvider == null)
            {
                throw new IllegalArgumentException("Native connection provider cannot be null");
            }

            if (myHostStates == null)
            {
                throw new IllegalArgumentException("Host states cannot be null");
            }

            if (myStatementDecorator == null)
            {
                throw new IllegalArgumentException("Statement decorator cannot be null");
            }

            return new CASLockFactory(this);
        }
    }

    private DistributedLock doTryLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
            throws LockException
    {
        LOG.trace("Trying lock for {} - {}", dataCenter, resource);

        if (!sufficientNodesForLocking(dataCenter, resource))
        {
            LOG.warn("Not sufficient nodes to lock resource {} in datacenter {}", resource, dataCenter);
            throw new LockException("Not sufficient nodes to lock");
        }

        try
        {
            CASLock casLock = new CASLock(dataCenter, resource, priority, metadata); // NOSONAR
            if (casLock.lock())
            {
                return casLock;
            }
        }
        catch (Exception e)
        {
            LOG.warn("Unable to lock resource {} in datacenter {} - {}", resource, dataCenter, e.getMessage());
            throw new LockException(e);
        }

        throw new LockException(String.format("Unable to lock resource %s in datacenter %s", resource, dataCenter));
    }

    private Set<Node> getNodesForResource(String dataCenter, String resource) throws UnsupportedEncodingException
    {
        Set<Node> dataCenterNodes = new HashSet<>();

        Metadata metadata = mySession.getMetadata();
        TokenMap tokenMap = metadata.getTokenMap()
                .orElseThrow(() -> new RuntimeException("Couldn't get tokenmap, is it disabled?")); //TODO
        Set<Node> nodes = tokenMap.getReplicas(myKeyspaceName, ByteBuffer.wrap(resource.getBytes("UTF-8")));

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

    private int liveNodes(Collection<Node> nodes)
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

    private ResultSet execute(String dataCenter, Statement statement) // NOPMD
    {
        //TODO
        /*Statement executeStatement;

        if (dataCenter != null && myRemoteRouting)
        {
            executeStatement = new DataCenterAwareStatement(statement, dataCenter);
        }
        else
        {
            executeStatement = statement;
        }

        return mySession.execute(myStatementDecorator.apply(executeStatement));*/
        return mySession.execute(statement);
    }

    private void verifySchemasExists()
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = mySession.getMetadata().getKeyspace(myKeyspaceName);
        if (!keyspaceMetadata.isPresent())
        {
            LOG.error("Keyspace {} does not exist, it needs to be created", myKeyspaceName);
            throw new IllegalStateException(
                    String.format("Keyspace %s does not exist, it needs to be created", myKeyspaceName));
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK).isPresent())
        {
            LOG.error("Table {}.{} does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK);
            throw new IllegalStateException(
                    String.format("Table %s.%s does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK));
        }

        if (!keyspaceMetadata.get().getTable(TABLE_LOCK_PRIORITY).isPresent())
        {
            LOG.error("Table {}.{} does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK_PRIORITY);
            throw new IllegalStateException(
                    String.format("Table %s.%s does not exist, it needs to be created", myKeyspaceName,
                            TABLE_LOCK_PRIORITY));
        }
    }

    class CASLock implements DistributedLock, Runnable
    {
        private final String myDataCenter;
        private final String myResource;
        private final int myPriority;
        private final Map<String, String> myMetadata;

        private final AtomicReference<ScheduledFuture<?>> myUpdateFuture = new AtomicReference<>();

        private final AtomicInteger myFailedUpdateAttempts = new AtomicInteger();

        private final int myLocallyHighestPriority;
        private final int globalHighPriority;

        CASLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
        {
            myDataCenter = dataCenter;
            myResource = resource;
            myPriority = priority;
            myMetadata = metadata;

            List<NodePriority> nodePriorities = computePriorities();

            myLocallyHighestPriority = nodePriorities.stream().filter(n -> n.getUuid().equals(myUuid))
                    .map(NodePriority::getPriority).findFirst().orElse(myPriority);
            globalHighPriority = nodePriorities.stream().filter(n -> !n.getUuid().equals(myUuid))
                    .map(NodePriority::getPriority).max(Integer::compare).orElse(myPriority);
        }

        public boolean lock()
        {
            if (compete())
            {
                LOG.trace("Trying to acquire lock for resource {}", myResource);
                if (tryLock())
                {
                    LOG.trace("Lock for resource {} acquired", myResource);
                    ScheduledFuture<?> future = myExecutor.scheduleAtFixedRate(this, LOCK_UPDATE_TIME_IN_SECONDS,
                            LOCK_UPDATE_TIME_IN_SECONDS, TimeUnit.SECONDS);
                    myUpdateFuture.set(future);

                    return true;
                }
            }

            return false;
        }

        @Override
        public void run()
        {
            try
            {
                updateLock();
                myFailedUpdateAttempts.set(0);
            }
            catch (Exception e)
            {
                int failedAttempts = myFailedUpdateAttempts.incrementAndGet();

                if (failedAttempts >= FAILED_LOCK_RETRY_ATTEMPTS)
                {
                    LOG.error("Unable to re-lock resource '{}' after {} failed attempts", myResource, failedAttempts);
                }
                else
                {
                    LOG.warn("Unable to re-lock resource '{}', {} failed attempts", myResource, failedAttempts, e);
                }
            }
        }

        @Override
        public void close()
        {
            ScheduledFuture<?> future = myUpdateFuture.get();
            if (future != null)
            {
                future.cancel(true);
                execute(myDataCenter, myRemoveLockStatement.bind(myResource, myUuid));

                if (myLocallyHighestPriority <= myPriority)
                {
                    execute(myDataCenter, myRemoveLockPriorityStatement.bind(myResource, myUuid));
                }
                else
                {
                    LOG.debug("Locally highest priority ({}) is higher than current ({}), will not remove",
                            myLocallyHighestPriority, myPriority);
                }
            }
        }

        private void updateLock() throws LockException
        {
            ResultSet resultSet = execute(myDataCenter,
                    myUpdateLockStatement.bind(myUuid, myMetadata, myResource, myUuid));

            if (!resultSet.wasApplied())
            {
                throw new LockException("CAS query failed");
            }
        }

        private boolean compete()
        {
            if (myLocallyHighestPriority <= myPriority)
            {
                insertPriority();
            }

            LOG.trace("Highest priority for resource {}: {}", myResource, globalHighPriority);
            return myPriority >= globalHighPriority;
        }

        private void insertPriority()
        {
            execute(myDataCenter, myCompeteStatement.bind(myResource, myUuid, myPriority));
        }

        private boolean tryLock()
        {
            return execute(myDataCenter, myLockStatement.bind(myResource, myUuid, myMetadata)).wasApplied();
        }

        private List<NodePriority> computePriorities()
        {
            List<NodePriority> nodePriorities = new ArrayList<>();

            ResultSet resultSet = execute(myDataCenter, myGetPriorityStatement.bind(myResource));

            for (Row row : resultSet)
            {
                int priority = row.getInt(COLUMN_PRIORITY);
                UUID hostId = row.getUuid(COLUMN_NODE);

                nodePriorities.add(new NodePriority(hostId, priority));
            }

            return nodePriorities;
        }

        int getFailedAttempts()
        {
            return myFailedUpdateAttempts.get();
        }
    }

    public static final class NodePriority
    {
        private final UUID myNode;
        private final int myPriority;

        public NodePriority(UUID node, int priority)
        {
            myNode = node;
            myPriority = priority;
        }

        public UUID getUuid()
        {
            return myNode;
        }

        public int getPriority()
        {
            return myPriority;
        }
    }
}
