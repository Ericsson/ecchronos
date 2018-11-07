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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TestTimeBasedRunPolicy extends AbstractCassandraTest
{
    @Parameterized.Parameters
    public static Collection<String> keyspaceNames() {
        return Arrays.asList("ecchronos", "anotherkeyspace");
    }

    private static final String TABLE_REJECT_CONFIGURATION = "reject_configuration";

    private static final long DEFAULT_REJECT_TIME = TimeUnit.MINUTES.toMillis(1);

    private static TimeBasedRunPolicy myRunPolicy;

    private static PreparedStatement insertRejectStatement;

    @Parameterized.Parameter
    public String myKeyspaceName;

    private ScheduledRepairJob myRepairJobMock;

    @Before
    public void initialize()
    {
        myRepairJobMock = mock(ScheduledRepairJob.class);
        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", myKeyspaceName));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.reject_configuration (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute))", myKeyspaceName));

        myRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(getNativeConnectionProvider().getSession())
                .withStatementDecorator(s -> s)
                .withKeyspaceName(myKeyspaceName)
                .build();
        insertRejectStatement = mySession.prepare(String.format("INSERT INTO %s.%s (keyspace_name, table_name, start_hour, start_minute, end_hour, end_minute) VALUES (?,?,?,?,?,?)", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
    }

    @After
    public void clear()
    {
        mySession.execute(String.format("TRUNCATE %s.%s", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
        myRunPolicy.close();
    }

    @Test
    public void testNonScheduledRepairJob()
    {
        ScheduledJob testJob = mock(ScheduledJob.class);

        assertThat(myRunPolicy.validate(testJob)).isEqualTo(-1L);
    }

    @Test
    public void testRejectedJob()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();
        DateTime start = now.minusHours(1);
        DateTime end = now.plusHours(1);

        long expectedHighest = end.getMillis() - now.getMillis();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, start, end);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isLessThanOrEqualTo(expectedHighest);
        assertThat(delay).isGreaterThan(-1L);
    }

    @Test
    public void testNonRejectedJobRejectBefore()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, now.minusHours(2), now.minusHours(1));

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
    }

    @Test
    public void testNonRejectedJobRejectAfter()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, now.plusHours(1), now.plusHours(2));

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
    }

    @Test
    public void testRejectedWraparound()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();
        DateTime start = now.plusHours(2);
        DateTime end = now.plusHours(1);

        long expectedHighest = end.getMillis() - now.getMillis();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, start, end);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isLessThanOrEqualTo(expectedHighest);
        assertThat(delay).isGreaterThan(-1L);
    }

    @Test
    public void testRejectedWraparoundWithStartBeforeNow()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();
        DateTime start = now.minusHours(1);
        DateTime end = now.minusHours(2).plusHours(24);

        long expectedHighest = end.getMillis() - now.getMillis();
        long expectedLowest = end.minusHours(1).getMillis() - now.getMillis();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, start, end);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isLessThanOrEqualTo(expectedHighest);
        assertThat(delay).isGreaterThanOrEqualTo(expectedLowest);
    }

    @Test
    public void testNonRejectedWraparound()
    {
        String keyspace = "test";
        String table = "table";
        DateTime now = new DateTime();

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, now.plusHours(2), now.minusHours(1));

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
    }

    @Test
    public void testRejectAnyTable()
    {
        String keyspace1 = "test";
        String keyspace2 = "test2";
        String table = "table";

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace1, table));
        insertEntry("*", table, 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace2, table));
        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
    }

    @Test
    public void testPausedJob()
    {
        String keyspace = "test";
        String table = "table";

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry(keyspace, table, 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
    }

    @Test
    public void testAllPaused()
    {
        String keyspace = "test";
        String table = "table";

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry("*", "*", 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
    }

    @Test
    public void testAllPausedIsCached()
    {
        String keyspace = "test";
        String table = "table";

        when(myRepairJobMock.getTableReference()).thenReturn(new TableReference(keyspace, table));
        insertEntry("*", "*", 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);

        mySession.execute(String.format("DELETE FROM %s.%s WHERE keyspace_name = '*' AND table_name = '*'", myKeyspaceName, TABLE_REJECT_CONFIGURATION));

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
    }

    @Test (expected = IllegalStateException.class)
    public void testBuildWithoutAllTablesCausesIllegalStateException()
    {
        try
        {
            mySession.execute(String.format("DROP TABLE %s.%s", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
            TimeBasedRunPolicy.builder()
                    .withSession(getNativeConnectionProvider().getSession())
                    .withStatementDecorator(s -> s)
                    .withKeyspaceName(myKeyspaceName)
                    .build();
        }
        finally
        {
            mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute))", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
        }
    }

    @Test (expected = NoHostAvailableException.class)
    public void testActivateWithoutCassandraCausesNoHostAvailableException()
    {
        // mock
        Session session = mock(Session.class);
        Cluster cluster = mock(Cluster.class);

        doThrow(NoHostAvailableException.class).when(cluster).getMetadata();
        doReturn(cluster).when(session).getCluster();

        // test
        TimeBasedRunPolicy.builder()
                .withSession(session)
                .withStatementDecorator(s -> s)
                .build();
    }

    private void insertEntry(String keyspace, String table, DateTime start, DateTime end)
    {
        insertEntry(keyspace, table, start.getHourOfDay(), start.getMinuteOfHour(), end.getHourOfDay(), end.getMinuteOfHour());
    }

    private void insertEntry(String keyspace, String table, int start_hour, int start_minute, int end_hour, int end_minute)
    {
        execute(insertRejectStatement.bind(
                keyspace,
                table,
                start_hour,
                start_minute,
                end_hour,
                end_minute));
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(statement);
    }
}
