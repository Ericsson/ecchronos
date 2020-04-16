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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.AbstractCreateStatement;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import net.jcip.annotations.NotThreadSafe;

@RunWith(MockitoJUnitRunner.class)
@NotThreadSafe
public class ITOnDemandRepairJob extends TestBase
{
    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private Metadata myMetadata;

    private Session mySession;

    private Host myLocalHost;

    private HostStatesImpl myHostStates;

    private RepairHistoryProviderImpl myRepairHistoryProvider;

    private OnDemandRepairSchedulerImpl myRepairSchedulerImpl;

    private ScheduleManagerImpl myScheduleManagerImpl;

    private CASLockFactory myLockFactory;

    private List<String> myCreatedKeyspaces = new ArrayList<>();

    @Before
    public void init()
    {
        myLocalHost = getNativeConnectionProvider().getLocalHost();
        mySession = getNativeConnectionProvider().getSession();
        Cluster cluster = mySession.getCluster();
        myMetadata = cluster.getMetadata();

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
                .withRunInterval(100, TimeUnit.MILLISECONDS)
                .build();
        ReplicationState replicationState = new ReplicationState(myMetadata, myLocalHost);
        myRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(replicationState)
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
                        .from("system_distributed", "repair_history")
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
     * Create a table that is replicated and schedule a repair on it
     */
    @Test
    public void repairSingleTable()
    {

        TableReference tableReference = new TableReference("ks", "tb");

        createKeyspace(tableReference.getKeyspace(), 3);
        createTable(tableReference);

        myRepairSchedulerImpl.scheduleJob(tableReference);

        long startTime = System.currentTimeMillis();
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
    }

    /**
     * Create two tables that are replicated and repair both
     */
    @Test
    public void repairMultipleTables()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = new TableReference("ks", "tb");
        TableReference tableReference2 = new TableReference("ks", "tb2");

        createKeyspace(tableReference.getKeyspace(), 3);
        createTable(tableReference);
        createTable(tableReference2);

        myRepairSchedulerImpl.scheduleJob(tableReference);
        myRepairSchedulerImpl.scheduleJob(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference2, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
    }

    /**
     * Schedule two jobs on the same table
     */
    @Test
    public void repairSameTableTwice()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = new TableReference("ks", "tb");

        createKeyspace(tableReference.getKeyspace(), 3);
        createTable(tableReference);

        myRepairSchedulerImpl.scheduleJob(tableReference);
        myRepairSchedulerImpl.scheduleJob(tableReference);
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);
        assertThat(myRepairSchedulerImpl.getCurrentRepairJobs()).isEmpty();
        verifyTableRepairedSince(tableReference, startTime, tokenRangesFor(tableReference.getKeyspace()).size() * 2);
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()).size());
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, int expectedTokenRanges)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(myTableRepairMetrics, times(expectedTokenRanges)).repairTiming(eq(tableReference), anyLong(),
                any(TimeUnit.class), eq(true));
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince).isPresent();
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

    private void createTable(TableReference tableReference)
    {
        AbstractCreateStatement createTableStatement = SchemaBuilder
                .createTable(tableReference.getKeyspace(), tableReference.getTable())
                .addPartitionKey("key", DataType.text())
                .addColumn("value", DataType.text());

        mySession.execute(createTableStatement);
    }

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
