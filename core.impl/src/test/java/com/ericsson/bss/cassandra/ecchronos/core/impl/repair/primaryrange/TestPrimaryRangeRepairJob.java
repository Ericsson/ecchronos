/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestPrimaryRangeRepairJob
{
    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long WARNING_IN_DAYS = 7;
    private static final long ERROR_IN_DAYS = 10;

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private RepairHistory.RepairSession mySession;
    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;
    @Mock
    private Node myNode;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairHistory.newSession(any(), any(), any(), any(), any(), any())).thenReturn(mySession);
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenReturn(OptionalLong.empty());
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS)
                .withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                // No initial delay, so that a table without history is due. The initial delay has tests of its own.
                .withInitialDelay(0, TimeUnit.MINUTES)
                .withRepairType(RepairType.PRIMARY_RANGE)
                .build();
    }

    private void withInitialDelay(final long hours)
    {
        myRepairConfiguration = RepairConfiguration.newBuilder(myRepairConfiguration)
                .withInitialDelay(hours, TimeUnit.HOURS)
                .build();
    }

    private void lastRepairedAt(final long timestamp)
    {
        when(myRepairHistoryProvider.lastSuccessfulRepair(eq(myNode), eq(myTableReference),
                eq(RepairType.PRIMARY_RANGE), anyLong(), anyLong())).thenReturn(OptionalLong.of(timestamp));
    }

    private PrimaryRangeRepairJob job()
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();
        return new PrimaryRangeRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withNode(myNode)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .build();
    }

    @Test
    public void testWithoutHistoryAndWithoutInitialDelayTheJobIsDueRightAway()
    {
        PrimaryRangeRepairJob job = job();

        assertThat(job.getRealPriority()).isGreaterThan(0);
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testWithoutHistoryTheFirstRepairWaitsForTheInitialDelay()
    {
        withInitialDelay(2);
        long before = System.currentTimeMillis();

        PrimaryRangeRepairJob job = job();

        // Like vnode repair: the table is taken for a new one, last repaired one interval minus the delay ago.
        long expected = before - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + TimeUnit.HOURS.toMillis(2);
        assertThat(job.getLastSuccessfulRun()).isBetween(expected, expected + 5_000);
        assertThat(job.getRealPriority()).isEqualTo(-1);
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testTheInitialDelayDecidesWhenTheFirstRepairIsDue()
    {
        withInitialDelay(2);
        long before = System.currentTimeMillis();

        ScheduledRepairJobView view = job().getView();

        // The next repair is the initial delay from now, not the interval.
        assertThat(view.getNextRepair()).isBetween(before + TimeUnit.HOURS.toMillis(2),
                before + TimeUnit.HOURS.toMillis(2) + 5_000);
    }

    @Test
    public void testHistoryTakesPrecedenceOverTheInitialDelay()
    {
        withInitialDelay(2);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) - 1000;
        lastRepairedAt(lastRepairedAt);

        PrimaryRangeRepairJob job = job();

        // A repair that is found is used as it is. It is overdue, so it is not delayed by the initial delay.
        assertThat(job.getLastSuccessfulRun()).isEqualTo(lastRepairedAt);
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testHistoryThatCouldNotBeReadDoesNotDelayTheRepair()
    {
        withInitialDelay(2);
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenThrow(new IllegalStateException("history unavailable"));

        PrimaryRangeRepairJob job = job();

        // A failing read says nothing about the table, so it must not be taken for a new one. It is repaired now.
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testDisabledHistoryIsTakenForNoHistoryAndHonoursTheInitialDelay()
    {
        withInitialDelay(2);
        myRepairConfiguration = RepairConfiguration.newBuilder(myRepairConfiguration)
                .withHistoryLookbackIntervals(0)
                .build();

        PrimaryRangeRepairJob job = job();

        // Like vnode repair: history_lookback_intervals 0 makes the table appear never repaired.
        verify(myRepairHistoryProvider, never()).lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong());
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testWithoutAHistoryProviderTheInitialDelayIsHonoured()
    {
        withInitialDelay(2);
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        PrimaryRangeRepairJob job = new PrimaryRangeRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withNode(myNode)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .build();

        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testRefreshStateDoesNotApplyTheInitialDelayAgain() throws InterruptedException
    {
        PrimaryRangeRepairJob job = job();
        long lastRun = job.getLastSuccessfulRun();
        assertThat(job.runnable()).isTrue();
        Thread.sleep(5);

        // Nothing is found in the history. That must not move the last run, which would reapply the delay.
        job.refreshState();

        assertThat(job.getLastSuccessfulRun()).isEqualTo(lastRun);
    }

    @Test
    public void testRecentRepairInHistoryMeansTheJobIsNotDue()
    {
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        lastRepairedAt(lastRepairedAt);

        PrimaryRangeRepairJob job = job();

        assertThat(job.getLastSuccessfulRun()).isEqualTo(lastRepairedAt);
        assertThat(job.getRealPriority()).isEqualTo(-1);
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testRepairOlderThanTheIntervalMeansTheJobIsDue()
    {
        lastRepairedAt(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) - 1000);

        assertThat(job().runnable()).isTrue();
    }

    @Test
    public void testHistoryIsReadForTheNodeTableAndTypeOfTheJob()
    {
        job();

        verify(myRepairHistoryProvider).lastSuccessfulRepair(eq(myNode), eq(myTableReference),
                eq(RepairType.PRIMARY_RANGE), anyLong(), anyLong());
    }

    @Test
    public void testHistoryIsNotReadThroughTheVnodeRangeIterator()
    {
        job();

        // Primary range history is not repair state of token ranges. It must not be read through iterate().
        verify(myRepairHistoryProvider, never()).iterate(any(), any(), anyLong(), any());
        verify(myRepairHistoryProvider, never()).iterate(any(), any(), anyLong(), anyLong(), any());
    }

    @Test
    public void testHistoryReadFailureLeavesTheJobDue()
    {
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenThrow(new IllegalStateException("history unavailable"));

        assertThat(job().runnable()).isTrue();
    }

    @Test
    public void testNoHistoryLookbackMeansHistoryIsNotRead()
    {
        myRepairConfiguration = RepairConfiguration.newBuilder(myRepairConfiguration)
                .withHistoryLookbackIntervals(0)
                .build();

        PrimaryRangeRepairJob job = job();

        verify(myRepairHistoryProvider, never()).lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong());
        // The fixture has no initial delay, so a table that appears never repaired is due.
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testRefreshStateDoesNotReadHistoryWhileNotDue()
    {
        lastRepairedAt(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
        PrimaryRangeRepairJob job = job();

        job.refreshState();
        job.refreshState();

        // Only the read at construction time.
        verify(myRepairHistoryProvider, times(1)).lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong());
    }

    @Test
    public void testRefreshStatePicksUpARepairThatFinishedElsewhere()
    {
        PrimaryRangeRepairJob job = job();
        assertThat(job.runnable()).isTrue();

        // An on-demand primary range repair finished after the job was created.
        long finishedAt = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
        lastRepairedAt(finishedAt);
        job.refreshState();

        assertThat(job.getLastSuccessfulRun()).isEqualTo(finishedAt);
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testRefreshStateNeverMovesTheLastRunBackwards()
    {
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) - 10_000;
        lastRepairedAt(lastRepairedAt);
        PrimaryRangeRepairJob job = job();

        lastRepairedAt(lastRepairedAt - TimeUnit.DAYS.toMillis(3));
        job.refreshState();

        assertThat(job.getLastSuccessfulRun()).isEqualTo(lastRepairedAt);
    }

    @Test
    public void testSuccessfulExecutionMakesTheJobNotDue()
    {
        PrimaryRangeRepairJob job = job();
        ScheduledTask task = job.iterator().next();
        assertThat(job.runnable()).isTrue();

        job.postExecute(TaskExecutionResult.SUCCESS, task);

        assertThat(job.getLastSuccessfulRun()).isGreaterThan(System.currentTimeMillis() - 5000);
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testFailedExecutionLeavesTheJobDue()
    {
        PrimaryRangeRepairJob job = job();
        ScheduledTask task = job.iterator().next();

        job.postExecute(TaskExecutionResult.TERMINAL, task);

        assertThat(job.getRealPriority()).isGreaterThan(0);
    }

    @Test
    public void testSuccessDoesNotWriteHistoryRowsForOtherReplicas()
    {
        PrimaryRangeRepairJob job = job();

        job.postExecute(TaskExecutionResult.SUCCESS, job.iterator().next());

        // Unlike incremental repair, a primary range repair only repairs the primary ranges of this node. Rows for
        // the other replicas would claim work that was not done. The row is written by the repair task's session.
        verify(myRepairHistory, never()).recordCompletedRepair(any(), any(), any(), any(), any(), anyLong(),
                anyLong(), any());
    }

    @Test
    public void testIteratorReturnsOneRepairGroupWithOnePrimaryRangeTask()
    {
        DriverNode replica = mock(DriverNode.class);
        when(replica.getId()).thenReturn(myNodeId);
        when(replica.getDatacenter()).thenReturn("DC1");
        when(myReplicationState.getReplicas(myTableReference, myNode)).thenReturn(ImmutableSet.of(replica));
        PrimaryRangeRepairJob job = job();

        Iterator<ScheduledTask> iterator = job.iterator();
        ScheduledTask task = iterator.next();

        assertThat(iterator.hasNext()).isFalse();
        assertThat(task).isInstanceOf(RepairGroup.class);
        Collection<RepairTask> repairTasks = ((RepairGroup) task).getRepairTasks(myNodeId);
        assertThat(repairTasks).hasSize(1);
        assertThat(repairTasks.iterator().next()).isInstanceOf(PrimaryRangeRepairTask.class);
        assertThat(task.getPriority()).isEqualTo(job.getRealPriority());
    }

    @Test
    public void testViewShowsTypeAndProgressOfARepairedTable()
    {
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        lastRepairedAt(lastRepairedAt);

        ScheduledRepairJobView view = job().getView();

        assertThat(view.getRepairType()).isEqualTo(RepairType.PRIMARY_RANGE);
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(1.0d);
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.COMPLETED);
    }

    @Test
    public void testViewShowsNoProgressWhenDue()
    {
        lastRepairedAt(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) - 1000);

        ScheduledRepairJobView view = job().getView();

        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.ON_TIME);
    }

    @Test
    public void testViewStatusLateAndOverdue()
    {
        lastRepairedAt(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(WARNING_IN_DAYS));
        assertThat(job().getView().getStatus()).isEqualTo(ScheduledRepairJobView.Status.LATE);

        lastRepairedAt(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS));
        assertThat(job().getView().getStatus()).isEqualTo(ScheduledRepairJobView.Status.OVERDUE);
    }

    @Test
    public void testViewStatusBlockedWhenDueButParked()
    {
        PrimaryRangeRepairJob job = job();
        job.setRunnableIn(TimeUnit.HOURS.toMillis(1));

        assertThat(job.getView().getStatus()).isEqualTo(ScheduledRepairJobView.Status.BLOCKED);
    }
}
