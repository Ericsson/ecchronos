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

import com.datastax.oss.driver.api.core.CqlSession;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class ITOnDemandRepairJob extends TestBase
{

    private static TableRepairMetrics mockTableRepairMetrics;

    private static Metadata myMetadata;

    private static HostStatesImpl myHostStates;

    private static RepairHistoryService myEccRepairHistory;

    private static OnDemandRepairSchedulerImpl myRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private final Set<TableReference> myRepairs = new HashSet<>();

    private static TableReferenceFactory myTableReferenceFactory;

    private static Node myLocalHost;

    private static CqlSession myAdminSession;

    @Before
    public void init() throws IOException
    {
        initialize();
        myLocalHost = getNode();
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        myMetadata = getSession().getMetadata();
        myAdminSession = getAdminNativeConnectionProvider().getCqlSession();

        myTableReferenceFactory = new TableReferenceFactoryImpl(getSession());

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        NodeResolver nodeResolver = new NodeResolverImpl(getSession());

        ReplicationState replicationState = new ReplicationStateImpl(nodeResolver, getSession());

        myEccRepairHistory = new RepairHistoryService(getSession(), replicationState, nodeResolver, 2_592_000_000L);

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withConsistencySerial(ConsistencyType.SERIAL)
                .build();

        List<UUID> localNodeIdList = Collections.singletonList(myLocalHost.getHostId());

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withNodeIDList(localNodeIdList)
                .withRunInterval(100, TimeUnit.MILLISECONDS)
                .build();

        myRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(replicationState)
                .withSession(getSession())
                .withRepairConfigurationFunction(RepairConfiguration.DEFAULT)
                .withRepairHistory(myEccRepairHistory)
                .withOnDemandStatus(new OnDemandStatus(getNativeConnectionProvider()))
                .build();
    }

    @After
    public void clean()
    {
        for (TableReference tableReference : myRepairs)
        {
            myAdminSession.execute(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name")
                    .isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name")
                    .isEqualTo(literal(tableReference.getTable()))
                    .build());
            for (Node node : myMetadata.getNodes().values())
            {
                myAdminSession.execute(QueryBuilder.deleteFrom("ecchronos", "repair_history")
                        .whereColumn("table_id")
                        .isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id")
                        .isEqualTo(literal(node.getHostId()))
                        .build());
                myAdminSession.execute(QueryBuilder.deleteFrom("ecchronos", "on_demand_repair_status")
                        .whereColumn("host_id")
                        .isEqualTo(literal(node.getHostId()))
                        .build());
            }
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
        closeConnections();
    }

    private static void closeConnections()
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
        Node node = myLocalHost;
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);

        schedule(tableReference, node.getHostId());

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> {
                    int queueSize = myRepairSchedulerImpl.getActiveRepairJobs().size();
                    System.out.println("[repairSingleTable] Queue size for node " + node.getHostId() + ": " + queueSize);
                    return myRepairSchedulerImpl.getActiveRepairJobs().isEmpty();
                });
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);

        verifyTableRepairedSince(tableReference, startTime, node);
    }

    /**
     * Create two tables that are replicated and repair both
     **/

    @Test
    public void repairMultipleTables() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();
        Node node = myLocalHost;
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        TableReference tableReference2 = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_TWO_NAME);

        schedule(tableReference, node.getHostId());
        schedule(tableReference2, node.getHostId());

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> myRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);

        verifyTableRepairedSince(tableReference, startTime, node);
        verifyTableRepairedSince(tableReference2, startTime, node);
    }

    /**
     * Schedule two jobs on the same table
     **/

    @Test
    public void repairSameTableTwice() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();
        Node node = myLocalHost;
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);

        schedule(tableReference, node.getHostId());
        schedule(tableReference, node.getHostId());

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> myRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);

        verifyTableRepairedSince(tableReference, startTime, tokenRangesFor(tableReference.getKeyspace(), node).size() * 2, node);
    }

    private void schedule(TableReference tableReference, UUID nodeId) throws EcChronosException
    {
        myRepairs.add(tableReference);
        myRepairSchedulerImpl.scheduleJob(tableReference, RepairType.VNODE, nodeId);
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, Node node)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace(), node).size(), node);
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, int expectedTokenRanges, Node node)
    {
        OptionalLong repairedAtWithCassandraHistory = lastRepairedSince(tableReference, repairedSince, myEccRepairHistory, node);
        assertThat(repairedAtWithCassandraHistory.isPresent()).isTrue();

        OptionalLong repairedAtWithEccHistory = lastRepairedSince(tableReference, repairedSince, myEccRepairHistory, node);
        assertThat(repairedAtWithEccHistory.isPresent()).isTrue();

        verify(mockTableRepairMetrics, times(expectedTokenRanges)).repairSession(eq(tableReference), anyLong(),
                any(TimeUnit.class), eq(true));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference,
                                           long repairedSince,
                                           RepairHistoryService repairHistoryProvider,
                                           Node node)
    {
        return lastRepairedSince(tableReference,
                repairedSince,
                tokenRangesFor(tableReference.getKeyspace(), node),
                repairHistoryProvider,
                node);
    }

    private OptionalLong lastRepairedSince(TableReference tableReference,
                                           long repairedSince,
                                           Set<LongTokenRange> expectedRepaired,
                                           RepairHistoryService repairHistoryProvider,
                                           Node node)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = repairHistoryProvider.iterate(node, tableReference,
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

    private Set<LongTokenRange> tokenRangesFor(String keyspace, Node node)
    {
        return myMetadata.getTokenMap()
                .get()
                .getTokenRanges(keyspace, node)
                .stream()
                .map(this::convertTokenRange)
                .collect(Collectors.toSet());
    }

    private boolean fullyRepaired(RepairEntry repairEntry)
    {
        return repairEntry.getParticipants().size() >= 2 && repairEntry.getStatus() == RepairStatus.SUCCESS;
    }

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
