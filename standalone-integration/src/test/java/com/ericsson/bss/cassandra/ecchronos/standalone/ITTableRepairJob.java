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

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.AbstractCreateStatement;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.*;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.collect.Sets;
import net.jcip.annotations.NotThreadSafe;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
@NotThreadSafe
public class ITTableRepairJob extends TestBase
{
    @Mock
    private RepairFaultReporter myFaultReporter;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private TableStorageStates myTableStorageStates;

    private Metadata myMetadata;

    private Session mySession;

    private Host myLocalHost;

    private HostStatesImpl myHostStates;

    private RepairHistoryProviderImpl myRepairHistoryProvider;

    private RepairConfiguration myRepairConfiguration;

    private RepairSchedulerImpl myRepairSchedulerImpl;

    private ScheduleManagerImpl myScheduleManagerImpl;

    private CASLockFactory myLockFactory;

    private TableReferenceFactory myTableReferenceFactory;

    private List<String> myCreatedKeyspaces = new ArrayList<>();

    @Before
    public void init()
    {
        myLocalHost = getNativeConnectionProvider().getLocalHost();
        mySession = getNativeConnectionProvider().getSession();
        Cluster cluster = mySession.getCluster();
        myMetadata = cluster.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(myMetadata);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();
        myRepairHistoryProvider = new RepairHistoryProviderImpl(mySession, s -> s, TimeUnit.DAYS.toMillis(30));

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
                .withHost(myLocalHost)
                .withHostStates(HostStatesImpl.builder()
                        .withJmxProxyFactory(getJmxProxyFactory())
                        .build())
                .withMetadata(myMetadata)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(myTableRepairMetrics)
                .withFaultReporter(myFaultReporter)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairStateFactory(repairStateFactory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(myTableStorageStates)
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(60, TimeUnit.MINUTES)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("100m")) // 100MiB
                .build();
    }

    @After
    public void clean()
    {
        for (String keyspace : myCreatedKeyspaces)
        {
            for (TableMetadata tableMetadata : myMetadata.getKeyspace(keyspace).getTables())
            {
                mySession.execute(QueryBuilder.delete()
                        .from("system_distributed","repair_history")
                        .where(QueryBuilder.eq("keyspace_name", keyspace))
                        .and(QueryBuilder.eq("columnfamily_name", tableMetadata.getName())));
            }

            mySession.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists());
        }
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

        createKeyspace("ks", 3);
        createTable("ks", "tb");
        TableReference tableReference = myTableReferenceFactory.forTable("ks", "tb");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was repaired two hours ago using sub ranges.
     *
     * The repair factory should detect the new table automatically and schedule it to run.
     * If the sub ranges are not detected the repair will be postponed for 1 hour based on repair configuration.
     */
    @Test
    public void repairSingleTableRepairedInSubRanges()
    {
        long startTime = System.currentTimeMillis();

        createKeyspace("ks", 3);
        createTable("ks", "tb");
        TableReference tableReference = myTableReferenceFactory.forTable("ks", "tb");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2), true);
        myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was repaired two hours ago.
     * It also has a simulated size of 10 GiB and a target repair size of 100 MiB
     * which should result in around 102 repair sessions.
     */
    @Test
    public void repairSingleTableInSubRanges()
    {
        long startTime = System.currentTimeMillis();

        createKeyspace("ks", 3);
        createTable("ks", "tb");
        TableReference tableReference = myTableReferenceFactory.forTable("ks", "tb");

        when(myTableStorageStates.getDataSize(eq(tableReference))).thenReturn(UnitConverter.toBytes("10g"));
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);

        BigInteger numberOfRanges = BigInteger.valueOf(UnitConverter.toBytes("10g")).divide(BigInteger.valueOf(UnitConverter.toBytes("100m"))); // 102

        Set<LongTokenRange> expectedRanges = splitTokenRanges(tableReference, numberOfRanges);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime, expectedRanges));

        verifyTableRepairedSinceWithSubRangeRepair(tableReference, startTime, expectedRanges);
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
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

        createKeyspace("ks", 3);
        createTable("ks", "tb");
        TableReference tableReference = myTableReferenceFactory.forTable("ks", "tb");
        createTable("ks", "tb2");
        TableReference tableReference2 = myTableReferenceFactory.forTable("ks", "tb2");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);

        injectRepairHistory(tableReference2, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));
        myRepairSchedulerImpl.putConfiguration(tableReference2, myRepairConfiguration);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime));
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference2, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    /**
     * Create a table that is replicated and was fully repaired two hours ago.
     *
     * It was also partially repaired by another node.
     *
     * The repair factory should detect the table automatically and schedule it to run on the ranges that were not repaired.
     */
    @Test
    public void partialTableRepair()
    {
        long startTime = System.currentTimeMillis();
        long expectedRepairedInterval = startTime - TimeUnit.HOURS.toMillis(1);

        createKeyspace("ks", 3);
        createTable("ks", "tb");
        TableReference tableReference = myTableReferenceFactory.forTable("ks", "tb");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(tableReference);
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30),
                expectedRepairedBefore);
        myRepairSchedulerImpl.putConfiguration(tableReference, myRepairConfiguration);

        Set<TokenRange> allTokenRanges = myMetadata.getTokenRanges("ks", myLocalHost);
        Set<LongTokenRange> expectedRepairedRanges = Sets.difference(convertTokenRanges(allTokenRanges), convertTokenRanges(expectedRepairedBefore));

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime, expectedRepairedRanges));

        verifyTableRepairedSince(tableReference, expectedRepairedInterval, expectedRepairedRanges);
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private void verifyTableRepairedSinceWithSubRangeRepair(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince, expectedRepaired);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(myTableRepairMetrics, timeout(5000)).lastRepairedAt(tableReference, repairedAt.getAsLong());

        int expectedTokenRanges = expectedRepaired.size();
        verify(myTableRepairMetrics, times(expectedTokenRanges)).repairTiming(eq(tableReference), anyLong(), any(TimeUnit.class), eq(true));
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(myTableRepairMetrics, timeout(5000)).lastRepairedAt(tableReference, repairedAt.getAsLong());

        int expectedTokenRanges = expectedRepaired.size();
        verify(myTableRepairMetrics, times(expectedTokenRanges)).repairTiming(eq(tableReference), anyLong(), any(TimeUnit.class), eq(true));
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        return lastRepairedSince(tableReference, repairedSince, expectedRepaired).isPresent();
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, System.currentTimeMillis(), repairedSince, repairEntry -> fullyRepaired(repairEntry) && expectedRepairedCopy.remove(repairEntry.getRange()));

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
                List<LongTokenRange> subRanges = new TokenSubRangeUtil(longTokenRange).generateSubRanges(tokensPerRange);

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

        Insert insert = QueryBuilder.insertInto("system_distributed", "repair_history")
                .value("keyspace_name", tableReference.getKeyspace())
                .value("columnfamily_name", tableReference.getTable())
                .value("participants", participants)
                .value("id", UUIDs.startOf(started_at))
                .value("started_at", new Date(started_at))
                .value("finished_at", new Date(finished_at))
                .value("range_begin", range_begin)
                .value("range_end", range_end)
                .value("status", "SUCCESS");

        mySession.execute(insert);
    }

    private void createKeyspace(String keyspaceName, int replicationFactor)
    {
        KeyspaceOptions createKeyspaceStatement = SchemaBuilder.createKeyspace(keyspaceName)
                .with()
                .replication(getReplicationMap(replicationFactor));

        mySession.execute(createKeyspaceStatement);

        myCreatedKeyspaces.add(keyspaceName);
    }

    private Map<String, Object> getReplicationMap(int replicationFactor)
    {
        Map<String, Object> replicationMap = new HashMap<>();

        replicationMap.put("class", "NetworkTopologyStrategy");
        replicationMap.put(myLocalHost.getDatacenter(), replicationFactor);

        return replicationMap;
    }

    private void createTable(String keyspace, String table)
    {
        AbstractCreateStatement createTableStatement = SchemaBuilder.createTable(keyspace, table)
                .addPartitionKey("key", DataType.text())
                .addColumn("value", DataType.text());

        mySession.execute(createTableStatement);
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
        Set<LongTokenRange> allRanges = convertTokenRanges(myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost));

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
