/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;

import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.EccRepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITOnDemandRepairJob extends TestBase
{
    @Parameterized.Parameters
    public static List<Boolean> routingOptions()
    {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter
    public Boolean myRemoteRoutingOption;

    private static TableRepairMetrics mockTableRepairMetrics;

    private static Metadata myMetadata;

    private static Node myLocalHost;

    private static HostStatesImpl myHostStates;

    private static EccRepairHistory myEccRepairHistory;

    private static RepairHistoryProviderImpl myRepairHistoryProvider;

    private static OnDemandRepairSchedulerImpl myRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private static TableReferenceFactory myTableReferenceFactory;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(Boolean remoteRoutingOption) throws IOException
    {
        myRemoteRouting = remoteRoutingOption;
        initialize();

        mockTableRepairMetrics = mock(TableRepairMetrics.class);

        myLocalHost = getNativeConnectionProvider().getLocalNode();
        CqlSession session = getNativeConnectionProvider().getSession();
        myMetadata = session.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        NodeResolver nodeResolver = new NodeResolverImpl(session);

        DriverNode localNode = nodeResolver.fromUUID(myLocalHost.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(nodeResolver, session, myLocalHost);

        myEccRepairHistory = EccRepairHistory.newBuilder()
                .withReplicationState(replicationState)
                .withLookbackTime(30, TimeUnit.DAYS)
                .withLocalNode(localNode)
                .withSession(session)
                .withStatementDecorator(s -> s)
                .build();

        myRepairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver, session, s -> s,
                TimeUnit.DAYS.toMillis(30));

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withStatementDecorator(s -> s)
                .build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(100, TimeUnit.MILLISECONDS)
                .build();

        myRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(replicationState)
                .withSession(session)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairHistory(myEccRepairHistory)
                .withOnDemandStatus(new OnDemandStatus(getNativeConnectionProvider()))
                .build();
    }

    @After
    public void clean()
    {
        CqlSession adminSession = getAdminNativeConnectionProvider().getSession();

        for (TableReference tableReference : myRepairs)
        {
            adminSession.execute(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                            .whereColumn("keyspace_name").isEqualTo(literal(tableReference.getKeyspace()))
                            .whereColumn("columnfamily_name").isEqualTo(literal(tableReference.getTable())).build());
            for (Node node : myMetadata.getNodes().values())
            {
                adminSession.execute(QueryBuilder.deleteFrom("ecchronos", "repair_history")
                        .whereColumn("table_id").isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id").isEqualTo(literal(node.getHostId()))
                        .build());
                adminSession.execute(QueryBuilder.deleteFrom("ecchronos", "on_demand_repair_status")
                        .whereColumn("host_id").isEqualTo(literal(node.getHostId()))
                        .build());
            }
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
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
     * Create a table that is replicated and schedule a repair on it
     */
    @Test
    public void repairSingleTable() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);

        verifyTableRepairedSince(tableReference, startTime);
    }

    /**
     * Create two tables that are replicated and repair both
     */
    @Test
    public void repairMultipleTables() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        TableReference tableReference2 = myTableReferenceFactory.forTable("test", "table2");

        schedule(tableReference);
        schedule(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
    }

    /**
     * Schedule two jobs on the same table
     */
    @Test
    public void repairSameTableTwice() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        schedule(tableReference);
        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);

        verifyTableRepairedSince(tableReference, startTime, tokenRangesFor(tableReference.getKeyspace()).size() * 2);
    }

    private void schedule(TableReference tableReference) throws EcChronosException
    {
        myRepairs.add(tableReference);
        myRepairSchedulerImpl.scheduleJob(tableReference, false);
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()).size());
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, int expectedTokenRanges)
    {
        OptionalLong repairedAtWithCassandraHistory = lastRepairedSince(tableReference, repairedSince,
                myRepairHistoryProvider);
        assertThat(repairedAtWithCassandraHistory.isPresent()).isTrue();

        OptionalLong repairedAtWithEccHistory = lastRepairedSince(tableReference, repairedSince, myEccRepairHistory);
        assertThat(repairedAtWithEccHistory.isPresent()).isTrue();

        verify(mockTableRepairMetrics, times(expectedTokenRanges)).repairSession(eq(tableReference), anyLong(),
                any(TimeUnit.class), eq(true));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince,
            RepairHistoryProvider repairHistoryProvider)
    {
        return lastRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()),
                repairHistoryProvider);
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired, RepairHistoryProvider repairHistoryProvider)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(tableReference,
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

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
