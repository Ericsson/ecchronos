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
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.PrimaryRangeOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestPrimaryRangeOnDemandRepairJob
{
    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myHostId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private OngoingJob myOngoingJob;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private Consumer<UUID> myHook;
    @Mock
    private Node myNode;
    @Mock
    private RepairHistory myRepairHistory;

    @Before
    public void setup()
    {
        when(myOngoingJob.getTableReference()).thenReturn(myTableReference);
        when(myOngoingJob.getJobId()).thenReturn(UUID.randomUUID());
        when(myOngoingJob.getHostId()).thenReturn(myHostId);
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.PRIMARY_RANGE);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    private PrimaryRangeOnDemandRepairJob job()
    {
        return job(3, 1000L);
    }

    private PrimaryRangeOnDemandRepairJob job(final int retryAttempts, final long retryBackoffMs)
    {
        return new PrimaryRangeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(myReplicationState)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withOnFinished(myHook)
                .withNode(myNode)
                .withRetryAttempts(retryAttempts)
                .withRetryBackoffMs(retryBackoffMs)
                .build();
    }

    @Test
    public void testViewOfAJobInTheQueue()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();

        OnDemandRepairJobView view = repairJob.getView();

        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getNodeId()).isEqualTo(myHostId);
        assertThat(view.getStatus()).isEqualTo(OnDemandRepairJobView.Status.IN_QUEUE);
        assertThat(view.getRepairType()).isEqualTo(RepairType.PRIMARY_RANGE);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
    }

    @Test
    public void testViewOfAFailedAndOfAFinishedJob()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();

        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.failed);
        assertThat(repairJob.getView().getStatus()).isEqualTo(OnDemandRepairJobView.Status.ERROR);

        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.finished);
        assertThat(repairJob.getView().getStatus()).isEqualTo(OnDemandRepairJobView.Status.COMPLETED);
    }

    @Test
    public void testThereIsOneHighestPriorityRepairGroupWithOnePrimaryRangeTask()
    {
        DriverNode replica = mock(DriverNode.class);
        when(myReplicationState.getReplicas(myTableReference, myNode)).thenReturn(ImmutableSet.of(replica));
        PrimaryRangeOnDemandRepairJob repairJob = job();

        Iterator<ScheduledTask> iterator = repairJob.iterator();
        ScheduledTask task = iterator.next();

        assertThat(iterator.hasNext()).isFalse();
        assertThat(task).isInstanceOf(RepairGroup.class);
        assertThat(task.getPriority()).isEqualTo(ScheduledJob.Priority.HIGHEST.getValue());
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        Collection<RepairTask> repairTasks = ((RepairGroup) task).getRepairTasks(myHostId);
        assertThat(repairTasks).hasSize(1);
        assertThat(repairTasks.iterator().next()).isInstanceOf(PrimaryRangeRepairTask.class);
    }

    @Test
    public void testJobIsRunnableUntilItsTaskHasRun()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testTerminalFailureFailsTheJob()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();

        repairJob.postExecute(TaskExecutionResult.TERMINAL, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
        assertThat(repairJob.getView().getStatus()).isEqualTo(OnDemandRepairJobView.Status.ERROR);
    }

    @Test
    public void testProgress()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();
        assertThat(repairJob.getProgress()).isEqualTo(0);

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testProgressWhenTheOngoingJobIsFinished()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.finished);

        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testFinishingASuccessfulJobFinishesTheOngoingJob()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();
        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        repairJob.finishJob();

        verify(myOngoingJob).finishJob();
        verify(myHook).accept(any(UUID.class));
    }

    @Test
    public void testFinishingAFailedJobFailsTheOngoingJob()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job();
        repairJob.postExecute(TaskExecutionResult.TERMINAL, repairJob.iterator().next());

        repairJob.finishJob();

        verify(myOngoingJob).failJob();
        verify(myHook).accept(any(UUID.class));
    }

    @Test
    public void testRetryableFailureReschedulesWithAFreshTask()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job(3, 1000L);
        ScheduledTask firstTask = repairJob.iterator().next();

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, firstTask);

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        assertThat(repairJob.isInBackoff()).isTrue();
        assertThat(repairJob.iterator().next()).isNotSameAs(firstTask);
    }

    @Test
    public void testRetrySucceedsOnTheSecondAttempt()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job(3, 1000L);
        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testExhaustedRetriesFailTheJob()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job(2, 1000L);
        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testNoRetryWhenOnlyOneAttemptIsAllowed()
    {
        PrimaryRangeOnDemandRepairJob repairJob = job(1, 1000L);

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }
}
