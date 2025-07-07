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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TestTimeBasedRunPolicy extends AbstractCassandraContainerTest
{
    @Parameterized.Parameters
    public static Collection<String> keyspaceNames() {
        return Arrays.asList("ecchronos", "anotherkeyspace");
    }
    
    private static final ZoneId UTC = ZoneId.of("UTC");
    
    private static final String TABLE_REJECT_CONFIGURATION = "reject_configuration";

    private static final long DEFAULT_REJECT_TIME = TimeUnit.MINUTES.toMillis(1);

    private static TimeBasedRunPolicy myRunPolicy;

    private static PreparedStatement insertRejectStatement;

    @Parameterized.Parameter
    public String myKeyspaceName;

    private TableRepairJob myRepairJobMock;

    @Before
    public void initialize()
    {
        myRepairJobMock = mock(TableRepairJob.class);
        mySession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 1}", myKeyspaceName));
        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.reject_configuration (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, dc_exclusion set<text>, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute))", myKeyspaceName));

        insertRejectStatement = mySession.prepare(String.format("INSERT INTO %s.%s (keyspace_name, table_name, start_hour, start_minute, end_hour, end_minute, dc_exclusion) VALUES (?,?,?,?,?,?,?)", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
    }

    @After
    public void clear()
    {
        mySession.execute(String.format("TRUNCATE %s.%s", myKeyspaceName, TABLE_REJECT_CONFIGURATION));

        if (myRunPolicy != null)
        {
            myRunPolicy.close();
        }
    }

    @Test
    public void testNonTableRepairJob()
    {
        ScheduledJob testJob = mock(ScheduledJob.class);

        assertThat(myRunPolicy.validate(testJob)).isEqualTo(-1L);
    }

    @Test
    public void testRejectedJob()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime end = now.withHour(5).withMinute(0).withSecond(0);
        long expectedDelay = Duration.between(LocalDateTime.now(clock), end).toMillis();

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 3, 0, 5, 0);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isEqualTo(expectedDelay);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();
    }

    @Test
    public void testNonRejectedJobRejectBefore()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 2, 0, 4, 30);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isTrue();
    }

    @Test
    public void testNonRejectedJobRejectAfter()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 5, 0, 6, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isTrue();
    }

    @Test
    public void testRejectedWraparoundBeforeEnd()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime end = now.withHour(5).withMinute(30).withSecond(0);
        long expectedDelay = Duration.between(LocalDateTime.now(clock), end).toMillis();

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 6, 30, 5, 30);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isEqualTo(expectedDelay);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();
    }

    @Test
    public void testRejectedWraparoundWithAfterEnd()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        LocalDateTime now = LocalDateTime.now(clock);
        LocalDateTime end = now.plusDays(1).withHour(2).withMinute(30).withSecond(0);
        long expectedDelay = Duration.between(LocalDateTime.now(clock), end).toMillis();

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 3, 30, 2, 30);

        long delay = myRunPolicy.validate(myRepairJobMock);

        assertThat(delay).isEqualTo(expectedDelay);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();
    }

    @Test
    public void testNonRejectedWraparound()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);

        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 6, 30, 3, 30);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(-1L);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isTrue();
    }

    @Test
    public void testRejectAnyTable()
    {
        String keyspace1 = "test";
        String keyspace2 = "test2";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace1, table));
        insertEntry("*", table, 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace1, table))).isFalse();

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace2, table));
        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace2, table))).isFalse();
    }

    @Test
    public void testPausedJob()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry(keyspace, table, 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();
    }

    @Test
    public void testAllPaused()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry("*", "*", 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();
    }

    @Test
    public void testAllPausedIsCached()
    {
        String keyspace = "test";
        String table = "table";
        Clock clock = Clock.fixed(Instant.parse("2020-02-24T04:30:24Z"), UTC);
        policyWithClock(clock);

        when(myRepairJobMock.getTableReference()).thenReturn(tableReference(keyspace, table));
        insertEntry("*", "*", 0, 0, 0, 0);

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();

        mySession.execute(String.format("DELETE FROM %s.%s WHERE keyspace_name = '*' AND table_name = '*'", myKeyspaceName, TABLE_REJECT_CONFIGURATION));

        assertThat(myRunPolicy.validate(myRepairJobMock)).isEqualTo(DEFAULT_REJECT_TIME);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isFalse();

        // Cache expires after 1 sec
        await().pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.SECONDS)
                .until(() -> myRunPolicy.validate(myRepairJobMock) == -1L);
        assertThat(myRunPolicy.shouldRun(tableReference(keyspace, table))).isTrue();
    }

    @Test
    public void testDefaultCacheExpireTime()
    {
        assertThat(TimeBasedRunPolicy.DEFAULT_CACHE_EXPIRE_TIME).isEqualTo(TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void testBuildWithoutAllTablesCausesIllegalStateException()
    {
        mySession.execute(String.format("DROP TABLE %s.%s", myKeyspaceName, TABLE_REJECT_CONFIGURATION));

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> TimeBasedRunPolicy.builder()
                        .withSession(getNativeConnectionProvider().getSession())
                        .withStatementDecorator(s -> s)
                        .withKeyspaceName(myKeyspaceName)
                        .build());

        mySession.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, dc_exclusion set<text>, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute))", myKeyspaceName, TABLE_REJECT_CONFIGURATION));
    }

    @Test
    public void testActivateWithoutCassandraCausesNoHostAvailableException()
    {
        // mock
        CqlSession session = mock(CqlSession.class);

        doThrow(NoNodeAvailableException.class).when(session).getMetadata();

        // test
        assertThatExceptionOfType(NoNodeAvailableException.class)
                .isThrownBy(() -> TimeBasedRunPolicy.builder()
                        .withSession(session)
                        .withStatementDecorator(s -> s)
                        .build());
    }

    private void policyWithClock(Clock clock)
    {
        myRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(getNativeConnectionProvider().getSession())
                .withStatementDecorator(s -> s)
                .withKeyspaceName(myKeyspaceName)
                .withCacheExpireTime(TimeUnit.SECONDS.toMillis(1))
                .withClock(clock)
                .build();
    }

    private void insertEntry(String keyspace, String table, int start_hour, int start_minute, int end_hour, int end_minute)
    {
        Set<String> dcExclusionSet = Collections.singleton("*");
        execute(insertRejectStatement.bind(
                keyspace,
                table,
                start_hour,
                start_minute,
                end_hour,
                end_minute,
                dcExclusionSet));
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(statement);
    }
}
