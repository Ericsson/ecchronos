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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.EccRepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TokenSubRangeUtil;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.collect.Sets;
import net.jcip.annotations.NotThreadSafe;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITSchedules extends TestBase
{
    enum RepairHistoryType
    {
        CASSANDRA, ECC
    }

    @Parameterized.Parameters
    public static Collection parameters()
    {
        return Arrays.asList(new Object[][] {
                { RepairHistoryType.CASSANDRA, true },
                { RepairHistoryType.CASSANDRA, false },
                { RepairHistoryType.ECC, true },
                { RepairHistoryType.ECC, false }
        });
    }

    @Parameterized.Parameter
    public RepairHistoryType myRepairHistoryType;

    @Parameterized.Parameter(1)
    public Boolean myRemoteRoutingOption;

    private static RepairFaultReporter mockFaultReporter;

    private static TableRepairMetrics mockTableRepairMetrics;

    private static TableStorageStates mockTableStorageStates;

    private static Metadata myMetadata;

    private static CqlSession myAdminSession;

    private static Node myLocalHost;

    private static HostStatesImpl myHostStates;

    private static DriverNode myLocalNode;

    private static RepairHistoryProvider myRepairHistoryProvider;

    private static NodeResolver myNodeResolver;

    private static RepairSchedulerImpl myRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private static RepairConfiguration myRepairConfiguration;

    private static TableReferenceFactory myTableReferenceFactory;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(RepairHistoryType repairHistoryType, Boolean remoteRoutingOption) throws IOException
    {
        myRemoteRouting = remoteRoutingOption;
        initialize();

        mockFaultReporter = mock(RepairFaultReporter.class);
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        mockTableStorageStates = mock(TableStorageStates.class);

        myAdminSession = getAdminNativeConnectionProvider().getSession();

        myLocalHost = getNativeConnectionProvider().getLocalNode();
        CqlSession session = getNativeConnectionProvider().getSession();
        myMetadata = session.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myNodeResolver = new NodeResolverImpl(session);
        myLocalNode = myNodeResolver.fromUUID(myLocalHost.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(myNodeResolver, session, myLocalHost);

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
                .withConsistencySerial(ConsistencyType.DEFAULT)
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

        TimeBasedRunPolicy myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(session)
                .withStatementDecorator(s -> s)
                .withLocalNode(myLocalHost)
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
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(60, TimeUnit.MINUTES)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("100m")) // 100MiB
                .build();
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();

        for (TableReference tableReference : myRepairs)
        {
            myRepairSchedulerImpl.removeConfiguration(tableReference);

            stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name").isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name").isEqualTo(literal(tableReference.getTable()))
                    .build()));
            for (Node node : myMetadata.getNodes().values())
            {
                stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("ecchronos", "repair_history")
                        .whereColumn("table_id").isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id").isEqualTo(literal(node.getHostId()))
                        .build()));
            }
        }

        for (CompletionStage<AsyncResultSet> stage : stages)
        {
            CompletableFutures.getUninterruptibly(stage);
        }
        myRepairs.clear();
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
        verifyRepairSessionMetrics(tableReference, tokenRangesFor(tableReference.getKeyspace()).size());
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
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
        verifyRepairSessionMetrics(tableReference, tokenRangesFor(tableReference.getKeyspace()).size());
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
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
        verifyRepairSessionMetrics(tableReference, expectedRanges.size());
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
    }

    /**
     * Create a table that is replicated and was repaired two hours ago.
     *
     * The repair factory should detect the new table automatically and schedule it to run.
     */
    @Test
    public void repairSingleTableInParallel()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(60, TimeUnit.MINUTES)
                .withRepairType(RepairOptions.RepairType.PARALLEL_VNODE)
                .build();
        schedule(tableReference, repairConfiguration);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyRepairSessionMetrics(tableReference, 3); // Amount of repair groups
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
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
        verifyRepairSessionMetrics(tableReference, tokenRangesFor(tableReference.getKeyspace()).size());
        verifyTableRepairedSince(tableReference2, startTime);
        verifyRepairSessionMetrics(tableReference2, tokenRangesFor(tableReference2.getKeyspace()).size());
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
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

        Set<TokenRange> allTokenRanges = myMetadata.getTokenMap().get()
                .getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Set<LongTokenRange> expectedRepairedRanges = Sets.difference(convertTokenRanges(allTokenRanges),
                convertTokenRanges(expectedRepairedBefore));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime, expectedRepairedRanges));

        verifyTableRepairedSince(tableReference, expectedRepairedInterval);
        verifyRepairSessionMetrics(tableReference, expectedRepairedRanges.size());
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
    }

    private void schedule(TableReference tableReference)
    {
        schedule(tableReference, myRepairConfiguration);
    }

    private void schedule(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        if (myRepairs.add(tableReference))
        {
            myRepairSchedulerImpl.putConfigurations(tableReference, Collections.singleton(repairConfiguration));
        }
    }

    private void verifyRepairSessionMetrics(TableReference tableReference, int times)
    {
        verify(mockTableRepairMetrics, atLeast(times))
                .repairSession(eq(tableReference), anyLong(), any(TimeUnit.class), eq(true));
    }

    private void verifyTableRepairedSinceWithSubRangeRepair(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince, expectedRepaired);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(mockTableRepairMetrics, timeout(5000)).lastRepairedAt(eq(tableReference), longThat(l -> l >= repairedAt.getAsLong()));
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(mockTableRepairMetrics, timeout(5000)).lastRepairedAt(eq(tableReference), longThat(l -> l >= repairedAt.getAsLong()));
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
        return myMetadata.getTokenMap().get().getTokenRanges(keyspace, myLocalHost).stream()
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
                myMetadata.getTokenMap().get().getTokenRanges(tableReference.getKeyspace(), myLocalHost), splitRanges);
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
            Set<InetAddress> participants = myMetadata.getTokenMap().get()
                    .getReplicas(tableReference.getKeyspace(), tokenRange).stream()
                    .map(node -> ((InetSocketAddress) node.getEndPoint().resolve()).getAddress())
                    .collect(Collectors.toSet());

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
                String start = Long.toString(((Murmur3Token) tokenRange.getStart()).getValue());
                String end = Long.toString(((Murmur3Token) tokenRange.getEnd()).getValue());
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

        SimpleStatement statement;

        if (myRepairHistoryType == RepairHistoryType.CASSANDRA)
        {
            statement = QueryBuilder.insertInto("system_distributed", "repair_history")
                    .value("keyspace_name", literal(tableReference.getKeyspace()))
                    .value("columnfamily_name", literal(tableReference.getTable()))
                    .value("participants", literal(participants))
                    .value("coordinator", literal(myLocalNode.getPublicAddress()))
                    .value("id", literal(Uuids.startOf(started_at)))
                    .value("started_at", literal(Instant.ofEpochMilli(started_at)))
                    .value("finished_at", literal(Instant.ofEpochMilli(finished_at)))
                    .value("range_begin", literal(range_begin))
                    .value("range_end", literal(range_end))
                    .value("status", literal("SUCCESS")).build();
        }
        else
        {
            Set<UUID> nodes = participants.stream()
                    .map(myNodeResolver::fromIp)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(DriverNode::getId)
                    .collect(Collectors.toSet());

            statement = QueryBuilder.insertInto("ecchronos", "repair_history")
                    .value("table_id", literal(tableReference.getId()))
                    .value("node_id", literal(myLocalNode.getId()))
                    .value("repair_id", literal(Uuids.startOf(finished_at)))
                    .value("job_id", literal(tableReference.getId()))
                    .value("coordinator_id", literal(myLocalNode.getId()))
                    .value("range_begin", literal(range_begin))
                    .value("range_end", literal(range_end))
                    .value("participants", literal(nodes))
                    .value("status", literal("SUCCESS"))
                    .value("started_at", literal(Instant.ofEpochMilli(started_at)))
                    .value("finished_at", literal(Instant.ofEpochMilli(finished_at))).build();
        }

        myAdminSession.execute(statement);
    }

    private Set<TokenRange> halfOfTokenRanges(TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = myMetadata.getTokenMap().get()
                .getTokenRanges(tableReference.getKeyspace(), myLocalHost);
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
                myMetadata.getTokenMap().get().getTokenRanges(tableReference.getKeyspace(), myLocalHost));

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
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
