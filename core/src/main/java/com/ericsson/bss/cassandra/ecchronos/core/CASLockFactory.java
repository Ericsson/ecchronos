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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.ericsson.bss.cassandra.ecchronos.connection.DataCenterAwareStatement;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

/**
 * Lock factory using Cassandras LWT (Compare-And-Set operations) to create and maintain locks.
 *
 * Expected keyspace/tables:
 * CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
 *
 * CREATE TABLE IF NOT EXISTS ecchronos.lock (
 * resource text,
 * node uuid,
 * metadata map<text,text>,
 * PRIMARY KEY(resource))
 * WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
 *
 * CREATE TABLE IF NOT EXISTS ecchronos.lock_priority(
 * resource text,
 * node uuid,
 * priority int,
 * PRIMARY KEY(resource, node))
 * WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
 */
public class CASLockFactory implements LockFactory, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(CASLockFactory.class);

    private static final String COLUMN_RESOURCE = "resource";
    private static final String COLUMN_NODE = "node";
    private static final String COLUMN_METADATA = "metadata";
    private static final String COLUMN_PRIORITY = "priority";

    private static final int LOCK_TIME_IN_SECONDS = 600;
    private static final long LOCK_UPDATE_TIME_IN_SECONDS = 60;
    private static final int FAILED_LOCK_RETRY_ATTEMPTS = (int) (LOCK_TIME_IN_SECONDS / LOCK_UPDATE_TIME_IN_SECONDS) - 1;

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private final UUID myUuid;

    private final ScheduledExecutorService myExecutor;

    private final StatementDecorator myStatementDecorator;
    private final HostStates myHostStates;

    private final Session mySession;
    private final String myKeyspaceName;
    private final PreparedStatement myCompeteStatement;
    private final PreparedStatement myGetPriorityStatement;
    private final PreparedStatement myLockStatement;
    private final PreparedStatement myGetLockMetadataStatement;
    private final PreparedStatement myRemoveLockStatement;
    private final PreparedStatement myUpdateLockStatement;
    private final PreparedStatement myRemoveLockPriorityStatement;

    private CASLockFactory(Builder builder)
    {
        myStatementDecorator = builder.myStatementDecorator;
        myHostStates = builder.myHostStates;
        myKeyspaceName = builder.myKeyspaceName;

        myExecutor = Executors.newSingleThreadScheduledExecutor();

        mySession = builder.myNativeConnectionProvider.getSession();

        verifySchemasExists();

        Insert insertLockStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_METADATA, bindMarker())
                .ifNotExists();

        Select.Where getLockMetadataStatement = QueryBuilder.select(COLUMN_METADATA)
                .from(myKeyspaceName, TABLE_LOCK)
                .where(eq(COLUMN_RESOURCE, bindMarker()));

        Delete.Conditions removeLockStatement = QueryBuilder.delete()
                .from(myKeyspaceName, TABLE_LOCK)
                .where(eq(COLUMN_RESOURCE, bindMarker()))
                .onlyIf(eq(COLUMN_NODE, bindMarker()));

        Update.Conditions updateLockStatement = QueryBuilder.update(myKeyspaceName, TABLE_LOCK)
                .with(set(COLUMN_NODE, bindMarker()))
                .and(set(COLUMN_METADATA, bindMarker()))
                .where(eq(COLUMN_RESOURCE, bindMarker()))
                .onlyIf(eq(COLUMN_NODE, bindMarker()));

        Insert competeStatement = QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .value(COLUMN_RESOURCE, bindMarker())
                .value(COLUMN_NODE, bindMarker())
                .value(COLUMN_PRIORITY, bindMarker());

        Select.Where getPriorityStatement = QueryBuilder.select(COLUMN_PRIORITY)
                .from(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .where(eq(COLUMN_RESOURCE, bindMarker()));

        Delete.Where removeLockPriorityStatement = QueryBuilder.delete()
                .from(myKeyspaceName, TABLE_LOCK_PRIORITY)
                .where(eq(COLUMN_RESOURCE, bindMarker()))
                .and(eq(COLUMN_NODE, bindMarker()));

        myLockStatement = mySession.prepare(insertLockStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        myGetLockMetadataStatement = mySession.prepare(getLockMetadataStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        myRemoveLockStatement = mySession.prepare(removeLockStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        myUpdateLockStatement = mySession.prepare(updateLockStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        myCompeteStatement = mySession.prepare(competeStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        myGetPriorityStatement = mySession.prepare(getPriorityStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        myRemoveLockPriorityStatement = mySession.prepare(removeLockPriorityStatement)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        UUID hostId = builder.myNativeConnectionProvider.getLocalHost().getHostId();

        if (hostId == null)
        {
            hostId = UUID.randomUUID();
            LOG.warn("Unable to determine local nodes host id, using {} instead", hostId);
        }

        myUuid = hostId;
    }

    @Override
    public DistributedLock tryLock(String dataCenter, String resource, int priority, Map<String, String> metadata) throws LockException
    {
        LOG.trace("Trying lock for {} - {}", dataCenter, resource);

        if (!sufficientNodesForLocking(dataCenter, resource))
        {
            LOG.error("Not sufficient nodes to lock resource {} in data center {}", resource, dataCenter);
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
            LOG.error("Unable to lock resource {} in data center {}", resource, dataCenter, e);
            throw new LockException(e);
        }

        throw new LockException(String.format("Unable to lock resource %s in data center %s", resource, dataCenter));
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
            LOG.error("Unable to retrieve metadata for resource {}", resource, e);
        }

        return null;
    }

    @Override
    public boolean sufficientNodesForLocking(String dataCenter, String resource)
    {
        try
        {
            Set<Host> hosts = getHostsForResource(dataCenter, resource);

            int quorum = hosts.size() / 2 + 1;
            int liveNodes = liveNodes(hosts);

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
    public void close()
    {
        myExecutor.shutdown();
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

    private Set<Host> getHostsForResource(String dataCenter, String resource) throws UnsupportedEncodingException
    {
        Set<Host> dataCenterHosts = new HashSet<>();

        Set<Host> hosts = mySession.getCluster().getMetadata().getReplicas(myKeyspaceName, ByteBuffer.wrap(resource.getBytes("UTF-8")));

        if (dataCenter != null)
        {
            Iterator<Host> iterator = hosts.iterator();

            while (iterator.hasNext())
            {
                Host host = iterator.next();

                if (dataCenter.equals(host.getDatacenter()))
                {
                    dataCenterHosts.add(host);
                }
            }

            return dataCenterHosts;
        }

        return hosts;
    }

    private int liveNodes(Collection<Host> hosts)
    {
        int live = 0;
        for (Host host : hosts)
        {
            if (myHostStates.isUp(host))
            {
                live++;
            }
        }
        return live;
    }

    private ResultSet execute(String dataCenter, Statement statement)
    {
        Statement executeStatement;

        if (dataCenter != null)
        {
            executeStatement = new DataCenterAwareStatement(statement, dataCenter);
        }
        else
        {
            executeStatement = statement;
        }

        return mySession.execute(myStatementDecorator.apply(executeStatement));
    }

    private void verifySchemasExists()
    {
        KeyspaceMetadata keyspaceMetadata = mySession.getCluster().getMetadata().getKeyspace(myKeyspaceName);
        if (keyspaceMetadata == null)
        {
            LOG.error("Keyspace {} does not exist, it needs to be created", myKeyspaceName);
            throw new IllegalStateException(String.format("Keyspace %s does not exist, it needs to be created", myKeyspaceName));
        }

        if (keyspaceMetadata.getTable(TABLE_LOCK) == null)
        {
            LOG.error("Table {}.{} does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK);
            throw new IllegalStateException(String.format("Table %s.%s does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK));
        }

        if (keyspaceMetadata.getTable(TABLE_LOCK_PRIORITY) == null)
        {
            LOG.error("Table {}.{} does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK_PRIORITY);
            throw new IllegalStateException(String.format("Table %s.%s does not exist, it needs to be created", myKeyspaceName, TABLE_LOCK_PRIORITY));
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

        CASLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
        {
            myDataCenter = dataCenter;
            myResource = resource;
            myPriority = priority;
            myMetadata = metadata;
        }

        public boolean lock()
        {
            if (compete())
            {
                LOG.trace("Trying to acquire lock for resource {}", myResource);
                if (tryLock())
                {
                    LOG.trace("Lock for resource {} acquired", myResource);
                    ScheduledFuture<?> future = myExecutor.scheduleAtFixedRate(this, LOCK_UPDATE_TIME_IN_SECONDS, LOCK_UPDATE_TIME_IN_SECONDS, TimeUnit.SECONDS);
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
                execute(myDataCenter, myRemoveLockPriorityStatement.bind(myResource, myUuid));
            }
        }

        private void updateLock() throws LockException
        {
            ResultSet resultSet = execute(myDataCenter, myUpdateLockStatement.bind(myUuid, myMetadata, myResource, myUuid));

            if (!resultSet.wasApplied())
            {
                throw new LockException("CAS query failed");
            }
        }

        private boolean compete()
        {
            insertPriority();

            int highestPriority = getHighestPriority();
            LOG.trace("Highest priority for resource {}: {}", myResource, highestPriority);
            return myPriority >= highestPriority;
        }

        private void insertPriority()
        {
            execute(myDataCenter, myCompeteStatement.bind(myResource, myUuid, myPriority));
        }

        private boolean tryLock()
        {
            return execute(myDataCenter, myLockStatement.bind(myResource, myUuid, myMetadata)).wasApplied();
        }

        private int getHighestPriority()
        {
            int highest = -1;

            ResultSet resultSet = execute(myDataCenter, myGetPriorityStatement.bind(myResource));

            for (Row row : resultSet)
            {
                int priority = row.getInt("priority");

                if (priority > highest)
                {
                    highest = priority;
                }
            }

            return highest;
        }

        int getFailedAttempts()
        {
            return myFailedUpdateAttempts.get();
        }
    }
}
