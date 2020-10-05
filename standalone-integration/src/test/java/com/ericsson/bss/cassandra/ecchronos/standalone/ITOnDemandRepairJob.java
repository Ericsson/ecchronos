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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class ITOnDemandRepairJob extends TestBase
{
    private static TableRepairMetrics mockTableRepairMetrics;

    private static Metadata myMetadata;

    private static Host myLocalHost;

    private static HostStatesImpl myHostStates;

    private static RepairHistoryProviderImpl myRepairHistoryProvider;

    private static OnDemandRepairSchedulerImpl myRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private static TableReferenceFactory myTableReferenceFactory;

    private Set<TableReference> myRepairs = new HashSet<>();

    @BeforeClass
    public static void init()
    {
        mockTableRepairMetrics = mock(TableRepairMetrics.class);

        myLocalHost = getNativeConnectionProvider().getLocalHost();
        Session session = getNativeConnectionProvider().getSession();
        Cluster cluster = session.getCluster();
        myMetadata = cluster.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(myMetadata);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myRepairHistoryProvider = new RepairHistoryProviderImpl(session, s -> s, TimeUnit.DAYS.toMillis(30));

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
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(replicationState)
                .withMetadata(myMetadata)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .build();
    }

    @After
    public void clean()
    {
        Session adminSession = getAdminNativeConnectionProvider().getSession();

        for (TableReference tableReference : myRepairs)
        {
            adminSession.execute(QueryBuilder.delete()
                    .from("system_distributed", "repair_history")
                    .where(QueryBuilder.eq("keyspace_name", tableReference.getKeyspace()))
                    .and(QueryBuilder.eq("columnfamily_name", tableReference.getTable())));
        }

        reset(mockTableRepairMetrics);
    }

    @AfterClass
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
                .until(() -> myRepairSchedulerImpl.getCurrentRepairJobs().isEmpty());
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
                .until(() -> myRepairSchedulerImpl.getCurrentRepairJobs().isEmpty());
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
                .until(() -> myRepairSchedulerImpl.getCurrentRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);

        verifyTableRepairedSince(tableReference, startTime, tokenRangesFor(tableReference.getKeyspace()).size() * 2);
    }

    private void schedule(TableReference tableReference) throws EcChronosException
    {
        myRepairs.add(tableReference);
        myRepairSchedulerImpl.scheduleJob(tableReference);
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()).size());
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, int expectedTokenRanges)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt.isPresent()).isTrue();

        verify(mockTableRepairMetrics, times(expectedTokenRanges)).repairTiming(eq(tableReference), anyLong(),
                any(TimeUnit.class), eq(true));
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

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
