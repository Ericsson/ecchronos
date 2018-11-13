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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ericsson.bss.cassandra.ecchronos.core.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

@NotThreadSafe
public class TestRepairHistoryProviderImpl extends AbstractCassandraTest
{
    private static final String KEYSPACE = "keyspace";
    private static final String TABLE = "table";

    private static RepairHistoryProviderImpl repairHistoryProvider;

    private static PreparedStatement myInsertRecordStatement;

    private final TableReference myTableReference = new TableReference(KEYSPACE, TABLE);

    @BeforeClass
    public static void startup()
    {
        myInsertRecordStatement = mySession.prepare("INSERT INTO system_distributed.repair_history (keyspace_name, columnfamily_name, participants, id, started_at, finished_at, range_begin, range_end, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

        repairHistoryProvider = new RepairHistoryProviderImpl(mySession, s -> s);
    }

    @After
    public void testCleanup()
    {
        mySession.execute(String.format("DELETE FROM system_distributed.repair_history WHERE keyspace_name = '%s' AND columnfamily_name = '%s'", KEYSPACE, TABLE));
    }

    @Test
    public void testIterateEmptyRepairHistory()
    {
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), Predicates.<RepairEntry> alwaysTrue());

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateNonAcceptedRepairHistory() throws UnknownHostException
    {
        insertRecord(KEYSPACE, TABLE, new LongTokenRange(0, 1));

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), Predicates.<RepairEntry> alwaysFalse());

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateAcceptedRepairHistory() throws UnknownHostException
    {
        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), 10, Sets.newHashSet(InetAddress.getLocalHost()), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), Predicates.<RepairEntry> alwaysTrue());

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIterateSuccessfulRepairHistory() throws UnknownHostException
    {
        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), 10, Sets.newHashSet(InetAddress.getLocalHost()), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), new SuccessfulRepairEntryPredicate(InetAddress.getLocalHost()));

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIterateFailedRepairHistory() throws UnknownHostException
    {
        insertRecord(KEYSPACE, TABLE, new LongTokenRange(0, 1), RepairStatus.FAILED);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), new SuccessfulRepairEntryPredicate(InetAddress.getLocalHost()));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateOtherNodesRepairHistory() throws UnknownHostException
    {
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("127.0.0.2")), new LongTokenRange(0, 1), System.currentTimeMillis() - 5, System.currentTimeMillis(), RepairStatus.SUCCESS);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, System.currentTimeMillis(), new SuccessfulRepairEntryPredicate(InetAddress.getLocalHost()));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateWithOlderHistory() throws UnknownHostException
    {
        long repair_end = System.currentTimeMillis() - 5000;
        long repair_start = repair_end - 5;

        long iterate_start = repair_end + 1000;
        long iterate_end = System.currentTimeMillis();

        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getLocalHost()), new LongTokenRange(0, 1), repair_start, repair_end, RepairStatus.SUCCESS);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, iterate_end, iterate_start, new SuccessfulRepairEntryPredicate(InetAddress.getLocalHost()));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateWithHistory() throws UnknownHostException
    {
        long repair_end = 10;
        long repair_start = repair_end - 5;

        long iterate_start = repair_start - 5;
        long iterate_end = repair_end + 5;

        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), repair_start, Sets.newHashSet(InetAddress.getLocalHost()), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, iterate_end, iterate_start, new SuccessfulRepairEntryPredicate(InetAddress.getLocalHost()));

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIPartiallyRepairedINegative() throws UnknownHostException
    {
        long repair_end = System.currentTimeMillis() - 5000;
        long repair_start = repair_end - 5;

        long iterate_start = repair_start - 5;
        long iterate_end = System.currentTimeMillis();

        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getLocalHost()), new LongTokenRange(0, 1), repair_start, repair_end, RepairStatus.UNKNOWN);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getLocalHost()), new LongTokenRange(0, 1), repair_start, repair_end, RepairStatus.FAILED);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getLocalHost()), new LongTokenRange(0, 1), repair_start, repair_end, RepairStatus.STARTED);

        Map<LongTokenRange, Collection<Host>> tokenToHostMap = new HashMap<>();
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, iterate_end, iterate_start, new FullyRepairedRepairEntryPredicate(tokenToHostMap));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testFullyRepairedPositive()
    {
        long repair_end = 10;
        long repair_start = repair_end - 5;

        long iterate_start = repair_start - 5;
        long iterate_end = repair_end + 10;

        Map<LongTokenRange, Collection<Host>> tokenToHostMap = new HashMap<>();
        Metadata metadata = mySession.getCluster().getMetadata();
        Set<Host> hosts = metadata.getAllHosts();
        Host host = hosts.iterator().next();

        List<RepairEntry> expectedRepairEntries = new ArrayList<>();

        expectedRepairEntries.add(new RepairEntry(new LongTokenRange(0, 1), repair_start, Sets.newHashSet(host.getAddress()), "SUCCESS"));
        expectedRepairEntries.add(new RepairEntry(new LongTokenRange(2, 3), repair_start + 1, Sets.newHashSet(host.getAddress()), "SUCCESS"));
        expectedRepairEntries.add(new RepairEntry(new LongTokenRange(4, 5), repair_start + 2, Sets.newHashSet(host.getAddress()), "SUCCESS"));

        for (RepairEntry repairEntry : expectedRepairEntries)
        {
            insertRecord(KEYSPACE, TABLE, repairEntry);
        }

        tokenToHostMap.put(new LongTokenRange(0, 1), Sets.newHashSet(hosts));
        tokenToHostMap.put(new LongTokenRange(2, 3), Sets.newHashSet(hosts));
        tokenToHostMap.put(new LongTokenRange(4, 5), Sets.newHashSet(hosts));

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, iterate_end, iterate_start, new FullyRepairedRepairEntryPredicate(tokenToHostMap));

        List<RepairEntry> actualRepairEntries = Lists.newArrayList(repairEntryIterator);

        assertThat(actualRepairEntries).containsExactlyElementsOf(expectedRepairEntries);
    }

    @Test
    public void testFullyRepairedNegative() throws UnknownHostException
    {
        long repair_end = System.currentTimeMillis() - 5000;
        long repair_start = repair_end - 5;

        long iterate_start = repair_start - 5;
        long iterate_end = System.currentTimeMillis();

        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.1")), new LongTokenRange(0, 1), repair_start, repair_end, RepairStatus.SUCCESS);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.2")), new LongTokenRange(2, 3), repair_start, repair_end, RepairStatus.SUCCESS);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.3")), new LongTokenRange(4, 5), repair_start, repair_end, RepairStatus.SUCCESS);

        Map<LongTokenRange, Collection<Host>> tokenToHostMap = new HashMap<>();
        Metadata metadata = mySession.getCluster().getMetadata();
        Set<Host> hosts = metadata.getAllHosts();

        tokenToHostMap.put(new LongTokenRange(0, 1), Sets.newHashSet(hosts));
        tokenToHostMap.put(new LongTokenRange(2, 3), Sets.newHashSet(hosts));
        tokenToHostMap.put(new LongTokenRange(4, 5), Sets.newHashSet(hosts));

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, iterate_end, iterate_start, new FullyRepairedRepairEntryPredicate(tokenToHostMap));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    private void insertRecord(String keyspace, String table, LongTokenRange range, RepairStatus repairStatus) throws UnknownHostException
    {
        insertRecord(keyspace, table, Sets.newHashSet(InetAddress.getLocalHost()), range, System.currentTimeMillis() - 5, System.currentTimeMillis(), repairStatus);
    }

    private void insertRecord(String keyspace, String table, LongTokenRange range) throws UnknownHostException
    {
        insertRecord(keyspace, table, range, RepairStatus.SUCCESS);
    }

    private void insertRecord(String keyspace, String table, Set<InetAddress> hosts, LongTokenRange range, long started, long finished, RepairStatus repairStatus)
    {
        mySession.execute(myInsertRecordStatement.bind(
                keyspace,
                table,
                hosts,
                UUIDs.startOf(started),
                new Date(started),
                new Date(finished),
                range.start.toString(),
                range.end.toString(),
                repairStatus.toString()
                ));
    }

    private void insertRecord(String keyspace, String table, RepairEntry repairEntry)
    {
        long finishedAt = repairEntry.getStartedAt() + 5;

        mySession.execute(myInsertRecordStatement.bind(
                keyspace,
                table,
                repairEntry.getParticipants(),
                UUIDs.startOf(repairEntry.getStartedAt()),
                new Date(repairEntry.getStartedAt()),
                new Date(finishedAt),
                repairEntry.getRange().start.toString(),
                repairEntry.getRange().end.toString(),
                repairEntry.getStatus().toString()
        ));
    }

    /**
     * Predicate that filters out repair entries which are not successful and doesn't include the provided host.
     */
    public static class SuccessfulRepairEntryPredicate implements Predicate<RepairEntry>
    {
        private final InetAddress myHostAddress;

        public SuccessfulRepairEntryPredicate(InetAddress hostAddress)
        {
            myHostAddress = hostAddress;
        }

        @Override
        public boolean apply(RepairEntry repairEntry)
        {
            return repairEntry.getParticipants().contains(myHostAddress) &&
                    RepairStatus.SUCCESS == repairEntry.getStatus();
        }
    }
}
