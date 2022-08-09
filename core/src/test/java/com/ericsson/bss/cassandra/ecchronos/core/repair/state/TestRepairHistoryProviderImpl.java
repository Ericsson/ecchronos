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

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.ericsson.bss.cassandra.ecchronos.core.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import net.jcip.annotations.NotThreadSafe;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@NotThreadSafe
public class TestRepairHistoryProviderImpl extends AbstractCassandraTest
{
    private static final String KEYSPACE = "keyspace";
    private static final String TABLE = "table";
    private static int LOOKBACK_TIME = 100;
    private static int CLOCK_TIME = 25;
    private static int STARTED_AT = CLOCK_TIME - 5;

    private static RepairHistoryProviderImpl repairHistoryProvider;

    private static PreparedStatement myInsertRecordStatement;

    private static DriverNode myLocalNode;

    private final TableReference myTableReference = tableReference(KEYSPACE, TABLE);

    @BeforeClass
    public static void startup()
    {
        myInsertRecordStatement = mySession.prepare(
                "INSERT INTO system_distributed.repair_history (keyspace_name, columnfamily_name, coordinator, participants, id, started_at, finished_at, range_begin, range_end, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        NodeResolver nodeResolver = new NodeResolverImpl(mySession);

        Collection<Node> nodes = mySession.getMetadata().getNodes().values();
        Node node = nodes.iterator().next();
        myLocalNode = nodeResolver.fromIp(node.getBroadcastAddress().get().getAddress()).get();

        repairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver, mySession, s -> s, LOOKBACK_TIME,
                mockClock(CLOCK_TIME));
    }

    @After
    public void testCleanup()
    {
        mySession.execute(String.format(
                "DELETE FROM system_distributed.repair_history WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                KEYSPACE, TABLE));
    }

    @Test
    public void testIterateEmptyRepairHistory()
    {
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, CLOCK_TIME,
                Predicates.<RepairEntry>alwaysTrue());

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateNonAcceptedRepairHistory()
    {
        insertRecord(KEYSPACE, TABLE, new LongTokenRange(0, 1));

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, CLOCK_TIME,
                Predicates.alwaysFalse());

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateAcceptedRepairHistory()
    {
        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), STARTED_AT,
                CLOCK_TIME, Sets.newHashSet(myLocalNode), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, CLOCK_TIME,
                Predicates.alwaysTrue());

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIterateLookbackTimeLimitation()
    {
        int timeOutsideLookback = CLOCK_TIME - LOOKBACK_TIME - 1;
        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), timeOutsideLookback,
                timeOutsideLookback, Sets.newHashSet(myLocalNode), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(myTableReference, CLOCK_TIME,
                Predicates.alwaysTrue());

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateSuccessfulRepairHistory()
    {
        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), STARTED_AT,
                CLOCK_TIME, Sets.newHashSet(myLocalNode), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, CLOCK_TIME, new SuccessfulRepairEntryPredicate(myLocalNode));

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIterateFailedRepairHistory()
    {
        insertRecord(KEYSPACE, TABLE, new LongTokenRange(0, 1), RepairStatus.FAILED);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, CLOCK_TIME, new SuccessfulRepairEntryPredicate(myLocalNode));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateOtherNodesRepairHistory() throws UnknownHostException
    {
        insertRecordWithAddresses(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("127.0.0.2")),
                new LongTokenRange(0, 1), STARTED_AT, CLOCK_TIME, RepairStatus.SUCCESS);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, CLOCK_TIME, new SuccessfulRepairEntryPredicate(myLocalNode));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateWithOlderHistory()
    {
        long finishedAt = CLOCK_TIME - 5000;
        long startedAt = finishedAt - 5;

        long iterateStart = finishedAt + 1000;
        long iterateEnd = CLOCK_TIME;

        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(myLocalNode), new LongTokenRange(0, 1), startedAt, finishedAt,
                RepairStatus.SUCCESS);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, iterateEnd, iterateStart, new SuccessfulRepairEntryPredicate(myLocalNode));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testIterateWithHistory()
    {
        long finishedAt = 10;
        long startedAt = finishedAt - 5;

        long iterateStart = startedAt - 5;
        long iterateEnd = finishedAt + 5;

        RepairEntry expectedRepairEntry = new RepairEntry(new LongTokenRange(0, 1), startedAt,
                finishedAt, Sets.newHashSet(myLocalNode), "SUCCESS");
        insertRecord(KEYSPACE, TABLE, expectedRepairEntry);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, iterateEnd, iterateStart, new SuccessfulRepairEntryPredicate(myLocalNode));

        assertThat(repairEntryIterator.hasNext()).isTrue();
        assertThat(repairEntryIterator.next()).isEqualTo(expectedRepairEntry);
    }

    @Test
    public void testIPartiallyRepairedINegative()
    {
        long finishedAt = CLOCK_TIME - 5000;
        long startedAt = finishedAt - 5;

        long iterateStart = startedAt - 5;
        long iterateEnd = CLOCK_TIME;

        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(myLocalNode), new LongTokenRange(0, 1), startedAt, finishedAt,
                RepairStatus.UNKNOWN);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(myLocalNode), new LongTokenRange(0, 1), startedAt, finishedAt,
                RepairStatus.FAILED);
        insertRecord(KEYSPACE, TABLE, Sets.newHashSet(myLocalNode), new LongTokenRange(0, 1), startedAt, finishedAt,
                RepairStatus.STARTED);

        Map<LongTokenRange, Collection<DriverNode>> tokenToNodeMap = new HashMap<>();
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, iterateEnd, iterateStart,
                        new FullyRepairedRepairEntryPredicate(tokenToNodeMap));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testFullyRepairedPositive()
    {
        long finishedAt = 10;
        long startedAt = finishedAt - 5;

        long iterateStart = startedAt - 5;
        long iterateEnd = finishedAt + 10;

        Map<LongTokenRange, Collection<DriverNode>> tokenToNodeMap = new HashMap<>();
        Metadata metadata = mySession.getMetadata();

        Collection<Node> nodes = metadata.getNodes().values();
        Node node = nodes.iterator().next();

        NodeResolver nodeResolver = new NodeResolverImpl(mySession);
        DriverNode resolvedNode = nodeResolver.fromIp(node.getBroadcastAddress().get().getAddress()).get();

        List<RepairEntry> expectedRepairEntries = new ArrayList<>();

        expectedRepairEntries
                .add(new RepairEntry(new LongTokenRange(0, 1), startedAt, finishedAt, Sets.newHashSet(resolvedNode),
                        "SUCCESS"));
        expectedRepairEntries.add(new RepairEntry(new LongTokenRange(2, 3), startedAt + 1,
                finishedAt + 1, Sets.newHashSet(resolvedNode), "SUCCESS"));
        expectedRepairEntries.add(new RepairEntry(new LongTokenRange(4, 5), startedAt + 2,
                finishedAt + 2, Sets.newHashSet(resolvedNode), "SUCCESS"));

        for (RepairEntry repairEntry : expectedRepairEntries)
        {
            insertRecord(KEYSPACE, TABLE, repairEntry);
        }

        tokenToNodeMap.put(new LongTokenRange(0, 1), Sets.newHashSet(resolvedNode));
        tokenToNodeMap.put(new LongTokenRange(2, 3), Sets.newHashSet(resolvedNode));
        tokenToNodeMap.put(new LongTokenRange(4, 5), Sets.newHashSet(resolvedNode));

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, iterateEnd, iterateStart,
                        new FullyRepairedRepairEntryPredicate(tokenToNodeMap));

        List<RepairEntry> actualRepairEntries = Lists.newArrayList(repairEntryIterator);

        assertThat(actualRepairEntries).containsExactlyElementsOf(expectedRepairEntries);
    }

    @Test
    public void testFullyRepairedNegative() throws UnknownHostException
    {
        long finishedAt = CLOCK_TIME - 5000;
        long startedAt = finishedAt - 5;

        long iterateStart = startedAt - 5;
        long iterateEnd = CLOCK_TIME;

        insertRecordWithAddresses(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.1")),
                new LongTokenRange(0, 1), startedAt, finishedAt, RepairStatus.SUCCESS);
        insertRecordWithAddresses(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.2")),
                new LongTokenRange(2, 3), startedAt, finishedAt, RepairStatus.SUCCESS);
        insertRecordWithAddresses(KEYSPACE, TABLE, Sets.newHashSet(InetAddress.getByName("198.162.0.3")),
                new LongTokenRange(4, 5), startedAt, finishedAt, RepairStatus.SUCCESS);

        Map<LongTokenRange, Collection<DriverNode>> tokenToNodeMap = new HashMap<>();
        Metadata metadata = mySession.getMetadata();
        Collection<Node> nodes = metadata.getNodes().values();

        NodeResolver nodeResolver = new NodeResolverImpl(mySession);
        Set<DriverNode> resolvedNodes = nodes.stream()
                .map(Node::getBroadcastAddress)
                .map(Optional::get)
                .map(InetSocketAddress::getAddress)
                .map(nodeResolver::fromIp)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

        tokenToNodeMap.put(new LongTokenRange(0, 1), resolvedNodes);
        tokenToNodeMap.put(new LongTokenRange(2, 3), resolvedNodes);
        tokenToNodeMap.put(new LongTokenRange(4, 5), resolvedNodes);

        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider
                .iterate(myTableReference, iterateEnd, iterateStart,
                        new FullyRepairedRepairEntryPredicate(tokenToNodeMap));

        assertThat(repairEntryIterator.hasNext()).isFalse();
    }

    @Test
    public void testInvalidRange()
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> repairHistoryProvider.iterate(myTableReference, 1, 2, Predicates.alwaysTrue()))
                .withMessageContaining("Invalid range when iterating");
    }

    private void insertRecord(String keyspace, String table, LongTokenRange range, RepairStatus repairStatus)
    {
        insertRecord(keyspace, table, Sets.newHashSet(myLocalNode), range, STARTED_AT, CLOCK_TIME, repairStatus);
    }

    private void insertRecord(String keyspace, String table, LongTokenRange range)
    {
        insertRecord(keyspace, table, range, RepairStatus.SUCCESS);
    }

    private void insertRecord(String keyspace, String table, Set<DriverNode> nodes, LongTokenRange range, long started,
            long finished, RepairStatus repairStatus)
    {
        Set<InetAddress> nodeAddresses = nodes.stream()
                .map(DriverNode::getPublicAddress)
                .collect(Collectors.toSet());

        insertRecordWithAddresses(keyspace, table, nodeAddresses, range, started, finished, repairStatus);
    }

    private void insertRecordWithAddresses(String keyspace, String table, Set<InetAddress> nodes, LongTokenRange range,
            long started, long finished, RepairStatus repairStatus)
    {
        mySession.execute(myInsertRecordStatement.bind(
                keyspace,
                table,
                nodes.stream().findFirst().get(),
                nodes,
                Uuids.startOf(started),
                Instant.ofEpochMilli(started),
                Instant.ofEpochMilli(finished),
                Long.toString(range.start),
                Long.toString(range.end),
                repairStatus.toString()
        ));
    }

    private void insertRecord(String keyspace, String table, RepairEntry repairEntry)
    {
        long finishedAt = repairEntry.getStartedAt() + 5;

        Set<InetAddress> nodeAddresses = repairEntry.getParticipants().stream()
                .map(DriverNode::getPublicAddress)
                .collect(Collectors.toSet());

        mySession.execute(myInsertRecordStatement.bind(
                keyspace,
                table,
                nodeAddresses.stream().findFirst().get(),
                nodeAddresses,
                Uuids.startOf(repairEntry.getStartedAt()),
                Instant.ofEpochMilli(repairEntry.getStartedAt()),
                Instant.ofEpochMilli(finishedAt),
                Long.toString(repairEntry.getRange().start),
                Long.toString(repairEntry.getRange().end),
                repairEntry.getStatus().toString()
        ));
    }

    /**
     * Predicate that filters out repair entries which are not successful and doesn't include the provided host.
     */
    public static class SuccessfulRepairEntryPredicate implements Predicate<RepairEntry>
    {
        private final DriverNode myNode;

        public SuccessfulRepairEntryPredicate(DriverNode node)
        {
            myNode = node;
        }

        @Override
        public boolean apply(RepairEntry repairEntry)
        {
            return repairEntry.getParticipants().contains(myNode) &&
                    RepairStatus.SUCCESS == repairEntry.getStatus();
        }
    }

    private static Clock mockClock(long millis)
    {
        Clock clockMock = mock(Clock.class);
        when(clockMock.millis()).thenReturn(millis);
        return clockMock;
    }
}
