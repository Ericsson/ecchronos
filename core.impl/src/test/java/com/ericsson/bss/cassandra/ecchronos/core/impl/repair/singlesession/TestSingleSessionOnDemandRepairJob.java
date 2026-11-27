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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.singlesession;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SingleSessionOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
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
public class TestSingleSessionOnDemandRepairJob
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

    private SingleSessionOnDemandRepairJob job()
    {
        return job(3, 1000L);
    }

    private SingleSessionOnDemandRepairJob job(final int retryAttempts, final long retryBackoffMs)
    {
        return new SingleSessionOnDemandRepairJob.Builder()
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
        SingleSessionOnDemandRepairJob repairJob = job();

        OnDemandRepairJobView view = repairJob.getView();

        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getNodeId()).isEqualTo(myHostId);
        assertThat(view.getStatus()).isEqualTo(OnDemandRepairJobView.Status.IN_QUEUE);
        assertThat(view.getRepairType()).isEqualTo(RepairType.PRIMARY_RANGE);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
    }

    @Test
    public void testAllReplicatedRangesJobRepairsWithAnAllReplicatedRangesTask() throws Exception
    {
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.ALL_REPLICATED_RANGES);
        when(myReplicationState.getReplicas(myTableReference, myNode)).thenReturn(ImmutableSet.of(mock(DriverNode.class)));
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        SingleSessionOnDemandRepairJob repairJob = new SingleSessionOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(RepairConfiguration.newBuilder()
                        .withRepairType(RepairType.ALL_REPLICATED_RANGES).build())
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(myReplicationState)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withNode(myNode)
                .build();

        assertThat(repairJob.getView().getRepairType()).isEqualTo(RepairType.ALL_REPLICATED_RANGES);
        RepairGroup group = (RepairGroup) repairJob.iterator().next();
        Collection<RepairTask> repairTasks = group.getRepairTasks(myHostId);
        assertThat(repairTasks).hasSize(1);
        assertThat(repairTasks.iterator().next().toString()).startsWith("ALL_REPLICATED_RANGES");
    }

    @Test
    public void testJobWhoseRepairTypeDiffersFromTheOngoingJobIsRejected()
    {
        // The repair group takes its type from the configuration. A mismatch would run another repair than the one
        // that was asked for and recorded.
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.ALL_REPLICATED_RANGES);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(this::job);
    }

    @Test
    public void testViewOfAFailedAndOfAFinishedJob()
    {
        SingleSessionOnDemandRepairJob repairJob = job();

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
        SingleSessionOnDemandRepairJob repairJob = job();

        Iterator<ScheduledTask> iterator = repairJob.iterator();
        ScheduledTask task = iterator.next();

        assertThat(iterator.hasNext()).isFalse();
        assertThat(task).isInstanceOf(RepairGroup.class);
        assertThat(task.getPriority()).isEqualTo(ScheduledJob.Priority.HIGHEST.getValue());
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        Collection<RepairTask> repairTasks = ((RepairGroup) task).getRepairTasks(myHostId);
        assertThat(repairTasks).hasSize(1);
        assertThat(repairTasks.iterator().next()).isInstanceOf(SingleSessionRepairTask.class);
    }

    @Test
    public void testJobIsRunnableUntilItsTaskHasRun()
    {
        SingleSessionOnDemandRepairJob repairJob = job();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testTerminalFailureFailsTheJob()
    {
        SingleSessionOnDemandRepairJob repairJob = job();

        repairJob.postExecute(TaskExecutionResult.TERMINAL, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
        assertThat(repairJob.getView().getStatus()).isEqualTo(OnDemandRepairJobView.Status.ERROR);
    }

    @Test
    public void testProgress()
    {
        SingleSessionOnDemandRepairJob repairJob = job();
        assertThat(repairJob.getProgress()).isEqualTo(0);

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testProgressWhenTheOngoingJobIsFinished()
    {
        SingleSessionOnDemandRepairJob repairJob = job();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.finished);

        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testFinishingASuccessfulJobFinishesTheOngoingJob()
    {
        SingleSessionOnDemandRepairJob repairJob = job();
        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        repairJob.finishJob();

        verify(myOngoingJob).finishJob();
        verify(myHook).accept(any(UUID.class));
    }

    @Test
    public void testFinishingAFailedJobFailsTheOngoingJob()
    {
        SingleSessionOnDemandRepairJob repairJob = job();
        repairJob.postExecute(TaskExecutionResult.TERMINAL, repairJob.iterator().next());

        repairJob.finishJob();

        verify(myOngoingJob).failJob();
        verify(myHook).accept(any(UUID.class));
    }

    @Test
    public void testRetryableFailureReschedulesWithAFreshTask()
    {
        SingleSessionOnDemandRepairJob repairJob = job(3, 1000L);
        ScheduledTask firstTask = repairJob.iterator().next();

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, firstTask);

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        assertThat(repairJob.isInBackoff()).isTrue();
        assertThat(repairJob.iterator().next()).isNotSameAs(firstTask);
    }

    @Test
    public void testRetrySucceedsOnTheSecondAttempt()
    {
        SingleSessionOnDemandRepairJob repairJob = job(3, 1000L);
        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        repairJob.postExecute(TaskExecutionResult.SUCCESS, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testExhaustedRetriesFailTheJob()
    {
        SingleSessionOnDemandRepairJob repairJob = job(2, 1000L);
        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testNoRetryWhenOnlyOneAttemptIsAllowed()
    {
        SingleSessionOnDemandRepairJob repairJob = job(1, 1000L);

        repairJob.postExecute(TaskExecutionResult.RETRYABLE, repairJob.iterator().next());

        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }
}
