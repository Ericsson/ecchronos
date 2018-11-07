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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory.DistributedLock;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TestCASLockFactory extends AbstractCassandraTest
{
    @Parameterized.Parameters
    public static Collection<String> keyspaceNames() {
        return Arrays.asList("ecchronos", "anotherkeyspace");
    }

    private static final String TABLE_LOCK = "lock";
    private static final String TABLE_LOCK_PRIORITY = "lock_priority";

    private static final String DATA_CENTER = "DC1";
    private static CASLockFactory myLockFactory;
    private static PreparedStatement myLockStatement;
    private static PreparedStatement myRemoveLockStatement;
    private static PreparedStatement myCompeteStatement;

    private static HostStates hostStates;

    @Parameterized.Parameter
    public String myKeyspaceName;

    @Before
    public void startup()
    {
        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", myKeyspaceName));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.lock (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0", myKeyspaceName));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.lock_priority (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0", myKeyspaceName));

        hostStates = mock(HostStates.class);
        when(hostStates.isUp(any(Host.class))).thenReturn(true);
        myLockFactory = new CASLockFactory.Builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(hostStates)
                .withStatementDecorator(s -> s)
                .withKeyspaceName(myKeyspaceName)
                .build();

        myLockStatement = mySession.prepare(String.format("INSERT INTO %s.%s (resource, node, metadata) VALUES (?, ?, ?) IF NOT EXISTS", myKeyspaceName, TABLE_LOCK))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        myRemoveLockStatement = mySession.prepare(String.format("DELETE FROM %s.%s WHERE resource=?", myKeyspaceName, TABLE_LOCK));
        myCompeteStatement = mySession.prepare(String.format("INSERT INTO %s.%s (resource, node, priority) VALUES (?, ?, ?)", myKeyspaceName, TABLE_LOCK_PRIORITY));
    }

    @After
    public void testCleanup()
    {
        execute(new SimpleStatement(String.format("DELETE FROM %s.%s WHERE resource='%s'", myKeyspaceName, TABLE_LOCK_PRIORITY, "lock")));
        execute(myRemoveLockStatement.bind("lock"));
        myLockFactory.close();
    }

    @Test
    public void testGetLock() throws LockException
    {
        try (DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<String, String>()))
        {
        }
    }

    @Test
    public void testGetGlobalLock()
    {
        try (DistributedLock lock = myLockFactory.tryLock(null, "lock", 1, new HashMap<String, String>()))
        {
        }
        catch (LockException e)
        {
            fail("Should not get exception");
        }
    }

    @Test (expected = LockException.class)
    public void testGlobalLockTakenThrowsException() throws LockException
    {
        execute(myLockStatement.bind("lock", UUID.randomUUID(), new HashMap<>()));

        try (DistributedLock lock = myLockFactory.tryLock(null, "lock", 1, new HashMap<String, String>()))
        {
        }
    }

    @Test (expected = LockException.class)
    public void testGetLockWithLowerPriority() throws LockException
    {
        execute(myCompeteStatement.bind("lock", UUID.randomUUID(), 2));

        try (DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<String, String>()))
        {

        }
    }

    @Test (expected = LockException.class)
    public void testGetAlreadyTakenLock() throws LockException
    {
        execute(myLockStatement.bind("lock", UUID.randomUUID(), new HashMap<>()));

        try (DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, new HashMap<String, String>()))
        {

        }
    }

    @Test
    public void testReadMetadata() throws LockException
    {
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("data", "something");

        try (DistributedLock lock = myLockFactory.tryLock(DATA_CENTER, "lock", 1, expectedMetadata))
        {
            Map<String, String> actualMetadata = myLockFactory.getLockMetadata(DATA_CENTER, "lock");

            assertThat(actualMetadata).isEqualTo(expectedMetadata);
        }
    }

    @Test
    public void testInterruptCasLockUpdate() throws InterruptedException
    {
        Map<String, String> metadata = new HashMap<>();

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try
        {
            Future<?> future = executorService.submit(myLockFactory.new CASLock(DATA_CENTER, "lock", 1, metadata));

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
        try (CASLockFactory.CASLock lockUpdateTask = myLockFactory.new CASLock(DATA_CENTER, "lock", 1, metadata))
        {
            for (int i = 0; i < 10; i++)
            {
                lockUpdateTask.run();
                assertThat(lockUpdateTask.getFailedAttempts()).isEqualTo(i + 1);
            }

            execute(myLockStatement.bind("lock", myLockFactory.getHostId(), new HashMap<>()));
            lockUpdateTask.run();
            assertThat(lockUpdateTask.getFailedAttempts()).isEqualTo(0);
        }
    }

    @Test
    public void testActivateWithoutAllTablesCausesIllegalStateException()
    {
        try
        {
            mySession.execute(String.format("DROP TABLE %s.%s", myKeyspaceName, TABLE_LOCK));
            try
            {
                CASLockFactory casLockFactory = new CASLockFactory.Builder()
                        .withNativeConnectionProvider(getNativeConnectionProvider())
                        .withHostStates(hostStates)
                        .withStatementDecorator(s -> s)
                        .withKeyspaceName(myKeyspaceName)
                        .build();
                fail();
            }
            catch (IllegalStateException e)
            {
                // Expected exception
            }
        }
        finally
        {
            mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0", myKeyspaceName, TABLE_LOCK));
        }
    }

    @Test
    public void testActivateWithoutCassandraCausesIllegalStateException()
    {
        // mock
        Session session = mock(Session.class);
        Cluster cluster = mock(Cluster.class);

        doThrow(NoHostAvailableException.class).when(cluster).getMetadata();
        doReturn(cluster).when(session).getCluster();

        // test
        try
        {
            CASLockFactory casLockFactory = new CASLockFactory.Builder()
                    .withNativeConnectionProvider(new NativeConnectionProvider()
                    {
                        @Override
                        public Session getSession()
                        {
                            return session;
                        }

                        @Override
                        public Host getLocalHost()
                        {
                            return null;
                        }
                    })
                    .withHostStates(hostStates)
                    .withStatementDecorator(s -> s)
                    .withKeyspaceName(myKeyspaceName)
                    .build();
            fail();
        }
        catch (NoHostAvailableException e)
        {
            // Expected exception
        }
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(statement);
    }
}
