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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.AbstractCassandraContainerTest;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@NotThreadSafe
@RunWith (Parameterized.class)
public class TestCASLockFactory extends AbstractCassandraContainerTest
{
    @Parameterized.Parameters
    public static Collection<String> keyspaceNames()
    {
        return Arrays.asList("ecchronos", "anotherkeyspace");
    }

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private static final String DATA_CENTER = "DC1";
    private static CASLockFactory myLockFactory;
    private static PreparedStatement myLockStatement;
    private static PreparedStatement myRemoveLockStatement;
    private static PreparedStatement myCompeteStatement;
    private static PreparedStatement myGetPrioritiesStatement;

    private static HostStates hostStates;

    @Parameterized.Parameter
    public String myKeyspaceName;

    @Before
    public void startup()
    {
        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", myKeyspaceName));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.lock (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.lock_priority (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName));

        hostStates = mock(HostStates.class);
        when(hostStates.isUp(any(Node.class))).thenReturn(true);
        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .build();

        myLockStatement = mySession.prepare(QueryBuilder.insertInto(myKeyspaceName, TABLE_LOCK)
                .value("resource", bindMarker())
                .value("node", bindMarker())
                .value("metadata", bindMarker())
                .ifNotExists()
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL));
        myRemoveLockStatement =
                mySession.prepare(String.format("DELETE FROM %s.%s WHERE resource=? IF EXISTS", myKeyspaceName, TABLE_LOCK));
        myCompeteStatement = mySession.prepare(
                String.format("INSERT INTO %s.%s (resource, node, priority) VALUES (?, ?, ?)", myKeyspaceName, TABLE_LOCK_PRIORITY));
        myGetPrioritiesStatement =
                mySession.prepare(String.format("SELECT * FROM %s.%s WHERE resource=?", myKeyspaceName, TABLE_LOCK_PRIORITY));
    }

    @After
    public void testCleanup()
    {
        execute(SimpleStatement.newInstance(
                String.format("DELETE FROM %s.%s WHERE resource='%s'", myKeyspaceName, TABLE_LOCK_PRIORITY, "lock")));
        execute(myRemoveLockStatement.bind("lock"));
        myLockFactory.close();
    }

    @Test
    public void testGetDefaultTimeToLiveFromLockTable() throws LockException
    {
        String alterLockTable = String.format("ALTER TABLE %s.%s WITH default_time_to_live = 1200;", myKeyspaceName, TABLE_LOCK);
        mySession.execute(alterLockTable);
        Node node = mock(Node.class);
        when(node.getHostId()).thenReturn(UUID.randomUUID());
        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .build();
        assertThat(myLockFactory.getCasLockFactoryCacheContext().getFailedLockRetryAttempts()).isEqualTo(9);
        assertThat(myLockFactory.getCasLockFactoryCacheContext().getLockUpdateTimeInSeconds()).isEqualTo(120);
    }

    @Test
    public void testGetLock() throws LockException
    {
        try (LockFactory.DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<String, String>(), UUID.randomUUID()))
        {
        }

        assertPriorityListEmpty("lock");
        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isEmpty();
    }

    @Test
    public void testGetGlobalLock() throws LockException
    {
        try (LockFactory.DistributedLock lock = myLockFactory.tryLock(null, "lock", 1, new HashMap<String, String>(), UUID.randomUUID()))
        {
        }
        assertPriorityListEmpty("lock");
        assertThat(myLockFactory.getCachedFailure(null, "lock")).isEmpty();
    }

    @Test
    public void testGlobalLockTakenThrowsException()
    {
        UUID nodeId= UUID.randomUUID();
        execute(myLockStatement.bind("lock", nodeId, new HashMap<>()));

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockFactory.tryLock(null, "lock", 1, new HashMap<>(), nodeId));
        assertPrioritiesInList("lock", 1);
        assertThat(myLockFactory.getCachedFailure(null, "lock")).isNotEmpty();
    }

    @Test
    public void testGlobalLockTakenIsCachedOnSecondTry() throws AttributeNotFoundException,
                                                         InstanceNotFoundException,
                                                         MalformedObjectNameException,
                                                         MBeanException,
                                                         ReflectionException,
                                                         UnsupportedOperationException,
                                                         IOException,
                                                         InterruptedException
    {
        UUID nodeId= UUID.randomUUID();
        execute(myLockStatement.bind("lock", nodeId, new HashMap<>()));
        InternalDriverContext driverContext = (InternalDriverContext) mySession.getContext();
        //Check that no in-flight queries exist, we want all previous queries to complete before we proceed
        Optional<Node> connectedNode = driverContext.getPoolManager().getPools().keySet().stream().findFirst();
        while (getInFlightQueries(connectedNode.get()) != 0)
        {
            Thread.sleep(100);
        }
        long expectedLockReadCount = getReadCount(TABLE_LOCK) + 2; // We do a read due to CAS and execCommandOnContainer
        long expectedLockWriteCount = getWriteCount(TABLE_LOCK) + 1; // No writes as the lock is already held
        long expectedLockPriorityReadCount = getReadCount(TABLE_LOCK_PRIORITY) + 2; // We read the priorities
        long expectedLockPriorityWriteCount = getWriteCount(TABLE_LOCK_PRIORITY) + 1; // We update our local priority once

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockFactory.tryLock(null, "lock", 2, new HashMap<>(), nodeId));
        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockFactory.tryLock(null, "lock", 1, new HashMap<>(), nodeId));

        assertThat(getReadCount(TABLE_LOCK_PRIORITY)).isEqualTo(expectedLockPriorityReadCount);
        assertThat(getWriteCount(TABLE_LOCK_PRIORITY)).isEqualTo(expectedLockPriorityWriteCount);

        assertThat(getReadCount(TABLE_LOCK)).isEqualTo(expectedLockReadCount);
        assertThat(getWriteCount(TABLE_LOCK)).isEqualTo(expectedLockWriteCount);
        assertPrioritiesInList("lock", 2);
        assertThat(myLockFactory.getCachedFailure(null, "lock")).isNotEmpty();
    }

    private int getInFlightQueries(Node node)
    {
        int inFlightQueries = 0;
        Optional<Metrics> metrics = mySession.getMetrics();
        if (metrics.isPresent())
        {
            Optional<Metric> inFlight = metrics.get().getNodeMetric(node, DefaultNodeMetric.IN_FLIGHT);
            if (inFlight.isPresent())
            {
                inFlightQueries = (int) ((Gauge) inFlight.get()).getValue();
            }
        }
        return inFlightQueries;
    }

    @Test
    public void testGetLockWithLowerPriority()
    {
        execute(myCompeteStatement.bind("lock", UUID.randomUUID(), 2));

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<>(), UUID.randomUUID()));
        assertPrioritiesInList("lock", 1, 2);
        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isNotEmpty();
    }

    @Test
    public void testGetAlreadyTakenLock()
    {
        UUID nodeId= UUID.randomUUID();
        execute(myLockStatement.bind("lock", nodeId, new HashMap<>()));

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<>(), nodeId));
        assertPrioritiesInList("lock", 1);
        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isNotEmpty();
    }

    @Test
    public void testGetLockWithLocallyHigherPriority() throws LockException
    {
        UUID localHostId = getNativeConnectionProvider().getNodes().values().stream().toList().get(0).getHostId();
        execute(myCompeteStatement.bind("lock", localHostId, 2));
        CASLockFactory lockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .build();

        try (LockFactory.DistributedLock lock = lockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<>(), localHostId))
        {
        }

        assertPrioritiesInList("lock", 2);
        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isEmpty();
    }

    @Test
    public void testGetLockWithLocallyLowerPriority() throws LockException
    {
        UUID localHostId = getNativeConnectionProvider().getNodes().values().stream().toList().get(0).getHostId();
        execute(myCompeteStatement.bind("lock", localHostId, 1));
        CASLockFactory lockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .build();
        try (LockFactory.DistributedLock lock = lockFactory.tryLock(DATA_CENTER, "lock", 2, new HashMap<>(), localHostId))
        {
        }

        assertPriorityListEmpty("lock");
        assertThat(lockFactory.getCachedFailure(DATA_CENTER, "lock")).isEmpty();
    }

    @Test
    public void testReadMetadata() throws LockException
    {
        UUID nodeId= UUID.randomUUID();
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("data", "something");

        try (LockFactory.DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, expectedMetadata, nodeId))
        {
            Map<String, String> actualMetadata = myLockFactory.getLockMetadata(DATA_CENTER, "lock");

            assertThat(actualMetadata).isEqualTo(expectedMetadata);
        }

        assertPriorityListEmpty("lock");
        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isEmpty();
    }

    @Test
    public void testInterruptCasLockUpdate() throws InterruptedException
    {
        Map<String, String> metadata = new HashMap<>();
        UUID nodeId= UUID.randomUUID();

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try
        {
            Future<?> future = executorService.submit(
                    new CASLock(
                            DATA_CENTER,
                            "lock",
                            1,
                            metadata,
                            nodeId,
                            myLockFactory.getCasLockStatement()));

            Thread.sleep(100);

            future.cancel(true);

            executorService.shutdown();
            assertThat(executorService.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        }
        finally
        {
            if (!executorService.isShutdown())
            {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    public void testFailedLockRetryAttempts()
    {
        Map<String, String> metadata = new HashMap<>();
        UUID nodeId= UUID.randomUUID();
        try (CASLock lockUpdateTask = new CASLock(
                DATA_CENTER,
                "lock",
                1,
                metadata,
                nodeId,
                myLockFactory.getCasLockStatement()))
        {
            for (int i = 0; i < 10; i++)
            {
                lockUpdateTask.run();
                assertThat(lockUpdateTask.getFailedAttempts()).isEqualTo(i + 1);
            }

            execute(myLockStatement.bind("lock", nodeId, new HashMap<>()));
            lockUpdateTask.run();
            assertThat(lockUpdateTask.getFailedAttempts()).isEqualTo(0);
        }

        assertThat(myLockFactory.getCachedFailure(DATA_CENTER, "lock")).isEmpty();
    }

    @Test
    public void testActivateWithoutKeyspaceCausesIllegalStateException()
    {
        mySession.execute(String.format("DROP KEYSPACE %s", myKeyspaceName));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new CASLockFactoryBuilder()
                        .withNativeConnectionProvider(getNativeConnectionProvider())
                        .withHostStates(hostStates)
                        .withKeyspaceName(myKeyspaceName)
                        .build());

        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", myKeyspaceName));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName, TABLE_LOCK));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName, TABLE_LOCK_PRIORITY));
    }

    @Test
    public void testActivateWithoutLockTableCausesIllegalStateException()
    {
        mySession.execute(String.format("DROP TABLE %s.%s", myKeyspaceName, TABLE_LOCK));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new CASLockFactoryBuilder()
                        .withNativeConnectionProvider(getNativeConnectionProvider())
                        .withHostStates(hostStates)
                        .withKeyspaceName(myKeyspaceName)
                        .build());

        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName, TABLE_LOCK));
    }

    @Test
    public void testActivateWithoutLockPriorityTableCausesIllegalStateException()
    {
        mySession.execute(String.format("DROP TABLE %s.%s", myKeyspaceName, TABLE_LOCK_PRIORITY));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> new CASLockFactoryBuilder()
                        .withNativeConnectionProvider(getNativeConnectionProvider())
                        .withHostStates(hostStates)
                        .withKeyspaceName(myKeyspaceName)
                        .build());

        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0",
                myKeyspaceName, TABLE_LOCK_PRIORITY));
    }

    @Test
    public void testActivateWithoutCassandraCausesIllegalStateException()
    {
        // mock
        CqlSession session = mock(CqlSession.class);

        doThrow(AllNodesFailedException.class).when(session).getMetadata();

        // test
        assertThatExceptionOfType(AllNodesFailedException.class)
                .isThrownBy(() -> new CASLockFactoryBuilder()
                        .withNativeConnectionProvider(new DistributedNativeConnectionProvider()
                        {
                            @Override
                            public CqlSession getCqlSession()
                            {
                                return session;
                            }

                            @Override
                            public Map<UUID, Node> getNodes()
                            {
                                return null;
                            }

                            @Override
                            public void addNode(Node myNode)
                            {
                            }

                            @Override
                            public void removeNode(Node myNode)
                            {
                            }

                            @Override
                            public Boolean confirmNodeValid(Node node)
                            {
                                return false;
                            }

                            @Override
                            public ConnectionType getConnectionType()
                            {
                                return null;
                            }
                        })
                        .withHostStates(hostStates)
                        .withKeyspaceName(myKeyspaceName)
                        .build());
    }

    @Test
    public void testDataCenterAwareAgentTypeWithDefaultSerialConsistency()
    {
        Node nodeMock = mock(Node.class);
        Map<UUID, Node> mockNodes = new HashMap<>();
        mockNodes.put(UUID.randomUUID(), nodeMock);
        DistributedNativeConnectionProvider connectionProviderMock = mock(DistributedNativeConnectionProvider.class);

        when(nodeMock.getHostId()).thenReturn(UUID.randomUUID());
        when(connectionProviderMock.getCqlSession()).thenReturn(mySession);
        when(connectionProviderMock.getNodes()).thenReturn(mockNodes);
        when(connectionProviderMock.getConnectionType()).thenReturn(ConnectionType.datacenterAware);

        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(getDataCenterAwareConnectionTypeProvider())
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .withConsistencySerial(ConsistencyType.DEFAULT)
                .build();

        assertEquals(ConsistencyLevel.LOCAL_SERIAL, myLockFactory.getSerialConsistencyLevel());
    }

    @Test
    public void testOtherThanDataCenterAwareAgentTypeWithDefaultSerialConsistency()
    {
        Node nodeMock = mock(Node.class);
        DistributedNativeConnectionProvider connectionProviderMock = mock(DistributedNativeConnectionProvider.class);

        Map<UUID, Node> mockNodes = new HashMap<>();
        mockNodes.put(UUID.randomUUID(), nodeMock);

        when(nodeMock.getHostId()).thenReturn(UUID.randomUUID());
        when(connectionProviderMock.getCqlSession()).thenReturn(mySession);
        when(connectionProviderMock.getNodes()).thenReturn(mockNodes);

        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(connectionProviderMock)
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .withConsistencySerial(ConsistencyType.DEFAULT)
                .build();

        assertEquals(ConsistencyLevel.SERIAL, myLockFactory.getSerialConsistencyLevel());
    }

    @Test
    public void testLocalSerialConsistency()
    {
        DistributedNativeConnectionProvider connectionProviderMock = mock(DistributedNativeConnectionProvider.class);
        Node nodeMock = mock(Node.class);
        Map<UUID, Node> mockNodes = new HashMap<>();
        mockNodes.put(UUID.randomUUID(), nodeMock);
        when(nodeMock.getHostId()).thenReturn(UUID.randomUUID());
        when(connectionProviderMock.getCqlSession()).thenReturn(mySession);
        when(connectionProviderMock.getNodes()).thenReturn(mockNodes);

        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(connectionProviderMock)
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .withConsistencySerial(ConsistencyType.LOCAL)
                .build();

        assertEquals(ConsistencyLevel.LOCAL_SERIAL, myLockFactory.getSerialConsistencyLevel());
    }

    @Test
    public void testSerialConsistency()
    {
        DistributedNativeConnectionProvider connectionProviderMock = mock(DistributedNativeConnectionProvider.class);
        Node nodeMock = mock(Node.class);
        Map<UUID, Node> mockNodes = new HashMap<>();
        mockNodes.put(UUID.randomUUID(), nodeMock);
        when(nodeMock.getHostId()).thenReturn(UUID.randomUUID());
        when(connectionProviderMock.getCqlSession()).thenReturn(mySession);
        when(connectionProviderMock.getNodes()).thenReturn(mockNodes);

        myLockFactory = new CASLockFactoryBuilder()
                .withNativeConnectionProvider(connectionProviderMock)
                .withHostStates(hostStates)
                .withKeyspaceName(myKeyspaceName)
                .withConsistencySerial(ConsistencyType.SERIAL)
                .build();

        assertEquals(ConsistencyLevel.SERIAL, myLockFactory.getSerialConsistencyLevel());
    }

    private void assertPriorityListEmpty(String resource)
    {
        assertThat(getPriorities(resource)).isEmpty();
    }

    private void assertPrioritiesInList(String resource, Integer... priorities)
    {
        assertThat(getPriorities(resource)).containsExactlyInAnyOrder(priorities);
    }

    private Set<Integer> getPriorities(String resource)
    {
        ResultSet resultSet = execute(myGetPrioritiesStatement.bind(resource));
        List<Row> rows = resultSet.all();

        return rows.stream().map(r -> r.getInt("priority")).collect(Collectors.toSet());
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(statement);
    }

    private long getReadCount(String tableName) throws
                                                IOException,
                                                UnsupportedOperationException,
                                                InterruptedException
    {
        return getReadCountFromTableStats(tableName);
    }

    private long getWriteCount(String tableName) throws
                                                 IOException,
                                                 UnsupportedOperationException,
                                                 InterruptedException
    {
        return getWriteCountFromTableStats(tableName);
    }

    private long getReadCountFromTableStats(String tableName) throws UnsupportedOperationException, IOException, InterruptedException
    {
        String tableStatsOutput =
                getContainerNode().execInContainer("nodetool", "tablestats", myKeyspaceName + "." + tableName).getStdout();
        long readCount = 0;
        Pattern readCountPattern = Pattern.compile("Read Count:\\s+(\\d+)");
        Matcher readCountMatcher = readCountPattern.matcher(tableStatsOutput);

        if (readCountMatcher.find())
        {
            readCount = Long.parseLong(readCountMatcher.group(1));
        }

        return readCount;
    }

    private long getWriteCountFromTableStats(String tableName) throws UnsupportedOperationException, IOException, InterruptedException
    {
        String tableStatsOutput =
                getContainerNode().execInContainer("nodetool", "tablestats", myKeyspaceName + "." + tableName).getStdout();
        long writeCount = 0;
        Pattern writeCountPattern = Pattern.compile("Write Count:\\s+(\\d+)");
        Matcher writeCountMatcher = writeCountPattern.matcher(tableStatsOutput);

        if (writeCountMatcher.find())
        {
            writeCount = Long.parseLong(writeCountMatcher.group(1));
        }

        return writeCount;
    }

    private DistributedNativeConnectionProvider getDataCenterAwareConnectionTypeProvider()
    {
        return new DistributedNativeConnectionProvider()
        {
            @Override
            public CqlSession getCqlSession()
            {
                return mySession;
            }

            @Override
            public Map<UUID, Node> getNodes()
            {
                return mySession.getMetadata().getNodes();
            }

            @Override
            public void addNode(Node myNode)
            {
            }

            @Override
            public void removeNode(Node myNode)
            {
            }

            @Override
            public Boolean confirmNodeValid(Node node)
            {
                return false;
            }

            @Override
            public ConnectionType getConnectionType()
            {
                return ConnectionType.datacenterAware;
            }
        };
    }
}
