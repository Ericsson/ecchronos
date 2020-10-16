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
package com.ericsson.bss.cassandra.ecchronos.standalone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.*;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.collect.Sets;

import net.jcip.annotations.NotThreadSafe;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITTableRepairJob extends TestBase
{
    enum RepairHistoryType
    {
        CASSANDRA, ECC
    }

    @Parameterized.Parameters
    public static List<RepairHistoryType> repairHistoryTypes()
    {
        return Arrays.asList(RepairHistoryType.values());
    }

    @Parameterized.Parameter
    public RepairHistoryType myRepairHistoryType;

    private static RepairFaultReporter mockFaultReporter;

    private static TableRepairMetrics mockTableRepairMetrics;

    private static TableStorageStates mockTableStorageStates;

    private static Metadata myMetadata;

    private static Session myAdminSession;

    private static Host myLocalHost;

    private static HostStatesImpl myHostStates;

    private static Node myLocalNode;

    private static RepairHistoryProvider myRepairHistoryProvider;

    private static NodeResolver myNodeResolver;

    private static RepairSchedulerImpl myRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private static RepairConfiguration myRepairConfiguration;

    private static TableReferenceFactory myTableReferenceFactory;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(RepairHistoryType repairHistoryType)
    {
        mockFaultReporter = mock(RepairFaultReporter.class);
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        mockTableStorageStates = mock(TableStorageStates.class);

        myAdminSession = getAdminNativeConnectionProvider().getSession();

        myLocalHost = getNativeConnectionProvider().getLocalHost();
        Session session = getNativeConnectionProvider().getSession();
        Cluster cluster = session.getCluster();
        myMetadata = cluster.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(myMetadata);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myNodeResolver = new NodeResolverImpl(myMetadata);
        myLocalNode = myNodeResolver.fromUUID(myLocalHost.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(myNodeResolver, myMetadata, myLocalHost);

        EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                .withReplicationState(replicationState)
                .withLookbackTime(30, TimeUnit.DAYS)
                .withLocalNode(myLocalNode)
                .withSession(session)
                .withStatementDecorator(s -> s)
                .build();

        if (repairHistoryType == RepairHistoryType.ECC)
        {
            myRepairHistoryProvider = eccRepairHistory;
        }
        else if (repairHistoryType == RepairHistoryType.CASSANDRA)
        {
            myRepairHistoryProvider = new RepairHistoryProviderImpl(myNodeResolver, session, s -> s,
                    TimeUnit.DAYS.toMillis(30));
        }
        else
        {
            throw new IllegalArgumentException("Unknown repair history type for test");
        }

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withStatementDecorator(s -> s)
                .build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(1, TimeUnit.SECONDS)
                .build();

        RepairStateFactoryImpl repairStateFactory = RepairStateFactoryImpl.builder()
                .withReplicationState(replicationState)
                .withHostStates(HostStatesImpl.builder()
                        .withJmxProxyFactory(getJmxProxyFactory())
                        .build())
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withTableRepairMetrics(mockTableRepairMetrics)
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withFaultReporter(mockFaultReporter)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairStateFactory(repairStateFactory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(mockTableStorageStates)
                .withRepairHistory(eccRepairHistory)
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(60, TimeUnit.MINUTES)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("100m")) // 100MiB
                .build();
    }

    @After
    public void clean()
    {
        List<ResultSetFuture> futures = new ArrayList<>();

        for (TableReference tableReference : myRepairs)
        {
            myRepairSchedulerImpl.removeConfiguration(tableReference);

            futures.add(myAdminSession.executeAsync(QueryBuilder.delete()
                    .from("system_distributed", "repair_history")
                    .where(QueryBuilder.eq("keyspace_name", tableReference.getKeyspace()))
                    .and(QueryBuilder.eq("columnfamily_name", tableReference.getTable()))));
            for (Host host : myMetadata.getAllHosts())
            {
                futures.add(myAdminSession.executeAsync(QueryBuilder.delete()
                        .from("ecchronos", "repair_history")
                        .where(QueryBuilder.eq("table_id", tableReference.getId()))
                        .and(QueryBuilder.eq("node_id", host.getHostId()))));
            }
        }

        for (ResultSetFuture future : futures)
        {
            future.getUninterruptibly();
        }

        reset(mockTableRepairMetrics);
        reset(mockFaultReporter);
        reset(mockTableStorageStates);
    }

    @Parameterized.AfterParam
    public static void closeConnections()
    {
        myHostStates.close();
        myRepairSchedulerImpl.close();
        myScheduleManagerImpl.close();
        myLockFactory.close();
    }

    /**
     * Create a table that is replicated and was repaired two hours ago.
     *
     * The repair factory should detect the new table automatically and schedule it to run.
     */
    @Test
    public void repairSingleTable()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was repaired two hours ago using sub ranges.
     *
     * The repair factory should detect the new table automatically and schedule it to run. If the sub ranges are not
     * detected the repair will be postponed for 1 hour based on repair configuration.
     */
    @Test
    public void repairSingleTableRepairedInSubRanges()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2), true);

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was repaired two hours ago. It also has a simulated size of 10 GiB and a
     * target repair size of 100 MiB which should result in around 102 repair sessions.
     */
    @Test
    public void repairSingleTableInSubRanges()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        when(mockTableStorageStates.getDataSize(eq(tableReference))).thenReturn(UnitConverter.toBytes("10g"));
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        schedule(tableReference);

        BigInteger numberOfRanges = BigInteger.valueOf(UnitConverter.toBytes("10g"))
                .divide(BigInteger.valueOf(UnitConverter.toBytes("100m"))); // 102

        Set<LongTokenRange> expectedRanges = splitTokenRanges(tableReference, numberOfRanges);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime, expectedRanges));

        verifyTableRepairedSinceWithSubRangeRepair(tableReference, startTime, expectedRanges);
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create two tables that are replicated and was repaired two and four hours ago.
     *
     * The repair factory should detect the new tables automatically and schedule them to run.
     */
    @Test
    public void repairMultipleTables()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        TableReference tableReference2 = myTableReferenceFactory.forTable("test", "table2");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        injectRepairHistory(tableReference2, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));

        schedule(tableReference);
        schedule(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference2, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was fully repaired two hours ago.
     *
     * It was also partially repaired by another node.
     *
     * The repair factory should detect the table automatically and schedule it to run on the ranges that were not
     * repaired.
     */
    @Test
    public void partialTableRepair()
    {
        long startTime = System.currentTimeMillis();
        long expectedRepairedInterval = startTime - TimeUnit.HOURS.toMillis(1);

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(tableReference);
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30),
                expectedRepairedBefore);

        Set<TokenRange> allTokenRanges = myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Set<LongTokenRange> expectedRepairedRanges = Sets.difference(convertTokenRanges(allTokenRanges),
                convertTokenRanges(expectedRepairedBefore));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime, expectedRepairedRanges));

        verifyTableRepairedSince(tableReference, expectedRepairedInterval, expectedRepairedRanges);
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    private void schedule(TableReference tableReference)
    {
        if (myRepairs.add(tableReference))
        {
            myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);
        }
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private void verifyTableRepairedSinceWithSubRangeRepair(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince, expectedRepaired);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(mockTableRepairMetrics, timeout(5000)).lastRepairedAt(tableReference, repairedAt.getAsLong());

        int expectedTokenRanges = expectedRepaired.size();
        verify(mockTableRepairMetrics, times(expectedTokenRanges))
                .repairTiming(eq(tableReference), anyLong(), any(TimeUnit.class), eq(true));
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(mockTableRepairMetrics, timeout(5000)).lastRepairedAt(tableReference, repairedAt.getAsLong());

        int expectedTokenRanges = expectedRepaired.size();
        verify(mockTableRepairMetrics, times(expectedTokenRanges))
                .repairTiming(eq(tableReference), anyLong(), any(TimeUnit.class), eq(true));
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        return lastRepairedSince(tableReference, repairedSince, expectedRepaired).isPresent();
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference,
                System.currentTimeMillis(), repairedSince,
                repairEntry -> fullyRepaired(repairEntry) && expectedRepairedCopy.remove(repairEntry.getRange()));

        List<RepairEntry> repairEntries = Lists.newArrayList(repairEntryIterator);

        Set<LongTokenRange> actuallyRepaired = repairEntries.stream()
                .map(RepairEntry::getRange)
                .collect(Collectors.toSet());

        if (expectedRepaired.equals(actuallyRepaired))
        {
            return repairEntries.stream().mapToLong(RepairEntry::getStartedAt).min();
        }
        else
        {
            return OptionalLong.empty();
        }
    }

    private Set<LongTokenRange> tokenRangesFor(String keyspace)
    {
        return myMetadata.getTokenRanges(keyspace, myLocalHost).stream()
                .map(this::convertTokenRange)
                .collect(Collectors.toSet());
    }

    private boolean fullyRepaired(RepairEntry repairEntry)
    {
        return repairEntry.getParticipants().size() == 3 && repairEntry.getStatus() == RepairStatus.SUCCESS;
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax)
    {
        injectRepairHistory(tableReference, timestampMax, false);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, boolean splitRanges)
    {
        injectRepairHistory(tableReference, timestampMax,
                myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost), splitRanges);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges)
    {
        injectRepairHistory(tableReference, timestampMax, tokenRanges, false);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges,
            boolean splitRanges)
    {
        long timestamp = timestampMax - 1;

        for (TokenRange tokenRange : tokenRanges)
        {
            Set<InetAddress> participants = myMetadata.getReplicas(tableReference.getKeyspace(), tokenRange).stream()
                    .map(Host::getAddress).collect(Collectors.toSet());

            if (splitRanges)
            {
                LongTokenRange longTokenRange = convertTokenRange(tokenRange);
                BigInteger tokensPerRange = longTokenRange.rangeSize().divide(BigInteger.TEN);
                List<LongTokenRange> subRanges = new TokenSubRangeUtil(longTokenRange)
                        .generateSubRanges(tokensPerRange);

                for (LongTokenRange subRange : subRanges)
                {
                    String start = Long.toString(subRange.start);
                    String end = Long.toString(subRange.end);
                    injectRepairHistory(tableReference, timestampMax, participants, start, end);
                }
            }
            else
            {
                String start = tokenRange.getStart().toString();
                String end = tokenRange.getEnd().toString();
                injectRepairHistory(tableReference, timestamp, participants, start, end);
            }

            timestamp--;
        }
    }

    private void injectRepairHistory(TableReference tableReference, long timestamp, Set<InetAddress> participants,
            String range_begin, String range_end)
    {
        long started_at = timestamp;
        long finished_at = timestamp + 5;

        Insert insert;

        if (myRepairHistoryType == RepairHistoryType.CASSANDRA)
        {
            insert = QueryBuilder.insertInto("system_distributed", "repair_history")
                    .value("keyspace_name", tableReference.getKeyspace())
                    .value("columnfamily_name", tableReference.getTable())
                    .value("participants", participants)
                    .value("id", UUIDs.startOf(started_at))
                    .value("started_at", new Date(started_at))
                    .value("finished_at", new Date(finished_at))
                    .value("range_begin", range_begin)
                    .value("range_end", range_end)
                    .value("status", "SUCCESS");
        }
        else
        {
            Set<UUID> nodes = participants.stream()
                    .map(myNodeResolver::fromIp)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(Node::getId)
                    .collect(Collectors.toSet());

            insert = QueryBuilder.insertInto("ecchronos", "repair_history")
                    .value("table_id", tableReference.getId())
                    .value("node_id", myLocalNode.getId())
                    .value("repair_id", UUIDs.startOf(started_at))
                    .value("job_id", tableReference.getId())
                    .value("coordinator_id", myLocalNode.getId())
                    .value("range_begin", range_begin)
                    .value("range_end", range_end)
                    .value("participants", nodes)
                    .value("status", "SUCCESS")
                    .value("started_at", new Date(started_at))
                    .value("finished_at", new Date(finished_at));
        }

        myAdminSession.execute(insert);
    }

    private Set<TokenRange> halfOfTokenRanges(TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Iterator<TokenRange> iterator = allTokenRanges.iterator();
        for (int i = 0; i < allTokenRanges.size() / 2 && iterator.hasNext(); i++)
        {
            halfOfRanges.add(iterator.next());
        }

        return halfOfRanges;
    }

    private Set<LongTokenRange> convertTokenRanges(Set<TokenRange> tokenRanges)
    {
        return tokenRanges.stream().map(this::convertTokenRange).collect(Collectors.toSet());
    }

    private Set<LongTokenRange> splitTokenRanges(TableReference tableReference, BigInteger numberOfRanges)
    {
        Set<LongTokenRange> allRanges = convertTokenRanges(
                myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost));

        BigInteger totalRangeSize = allRanges.stream()
                .map(LongTokenRange::rangeSize)
                .reduce(BigInteger.ZERO, BigInteger::add);

        BigInteger tokensPerSubRange = totalRangeSize.divide(numberOfRanges);

        return allRanges.stream()
                .flatMap(range -> new TokenSubRangeUtil(range).generateSubRanges(tokensPerSubRange).stream())
                .collect(Collectors.toSet());
    }

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
