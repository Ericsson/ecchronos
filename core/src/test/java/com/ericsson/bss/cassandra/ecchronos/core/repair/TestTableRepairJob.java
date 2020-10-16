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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith (MockitoJUnitRunner.class)
public class TestTableRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    private static final long HUNDRED_MB_IN_BYTES = 100 * 1024 * 1024;
    private static final long THOUSAND_MB_IN_BYTES = 1000 * 1024 * 1024;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private Metadata myMetadata;

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private RepairHistory.RepairSession myRepairSession;

    private TableRepairJob myRepairJob;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);
    private RepairConfiguration myRepairConfiguration;

    @Before
    public void startup()
    {
        doReturn(-1L).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(myRepairStateSnapshot).when(myRepairState).getSnapshot();

        doNothing().when(myRepairState).update();

        when(myRepairHistory.newSession(any(), any(), any(), any())).thenReturn(myRepairSession);

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withTargetRepairSizeInBytes(HUNDRED_MB_IN_BYTES)
                .build();

        myRepairJob = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRepairState(myRepairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(myTableStorageStates)
                .withRepairHistory(myRepairHistory)
                .build();
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myMetadata));
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myRepairState));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testPrevalidateNotRepairable()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairState, times(1)).update();
        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNeedRepair()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairState, times(1)).update();
        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNotRepairableThenRepairable()
    {
        // mock
        doReturn(false).doReturn(true).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isFalse();
        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairState, times(2)).update();
        verify(myRepairStateSnapshot, times(2)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsOverloadException()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        doThrow(new OverloadedException(null, "Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsException()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteRepaired()
    {
        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        myRepairJob.postExecute(true, null);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteRepairedWithFailure()
    {
        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        myRepairJob.postExecute(false, null);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepaired()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true, null);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepairedWithFailure()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(false, null);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteUpdateThrowsException()
    {
        // mock
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true, null);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
    }

    @Test
    public void testGetView()
    {
        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
        RepairJobView repairJobView = myRepairJob.getView();

        assertThat(repairJobView.getId()).isEqualTo(myTableReference.getId());
        assertThat(repairJobView.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJobView.getRepairConfiguration()).isEqualTo(myRepairConfiguration);
        assertThat(repairJobView.getRepairStateSnapshot()).isEqualTo(myRepairStateSnapshot);
        assertThat(repairJobView.getStatus()).isEqualTo(RepairJobView.Status.ERROR);
    }

    @Test
    public void testIterator()
    {
        LongTokenRange tokenRange = new LongTokenRange(0, 10);
        ImmutableSet<Node> replicas = ImmutableSet.of(mock(Node.class), mock(Node.class));
        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(tokenRange);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl
                .newBuilder(ImmutableList.of(new VnodeRepairState(tokenRange, replicas, 1234L)))
                .build();
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, vnodes);

        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroups(Collections.singletonList(replicaRepairGroup))
                .withLastRepairedAt(1234L)
                .withVnodeRepairStates(vnodeRepairStates)
                .build();
        when(myRepairState.getSnapshot()).thenReturn(repairStateSnapshot);

        Iterator<ScheduledTask> iterator = myRepairJob.iterator();

        ScheduledTask task = iterator.next();
        assertThat(task).isInstanceOf(RepairGroup.class);
        Collection<RepairTask> repairTasks = ((RepairGroup)task).getRepairTasks();

        assertThat(repairTasks).hasSize(1);
        RepairTask repairTask = repairTasks.iterator().next();
        assertThat(repairTask.getReplicas()).containsExactlyInAnyOrderElementsOf(replicas);
        assertThat(repairTask.getTokenRanges()).containsExactly(tokenRange);
        assertThat(repairTask.getRepairConfiguration()).isEqualTo(myRepairConfiguration);
        assertThat(repairTask.getTableReference()).isEqualTo(myTableReference);
    }

    @Test
    public void testIteratorWithTargetSize()
    {
        List<LongTokenRange> expectedTokenRanges = Arrays.asList(
                new LongTokenRange(0, 1),
                new LongTokenRange(1, 2),
                new LongTokenRange(2, 3),
                new LongTokenRange(3, 4),
                new LongTokenRange(4, 5),
                new LongTokenRange(5, 6),
                new LongTokenRange(6, 7),
                new LongTokenRange(7, 8),
                new LongTokenRange(8, 9),
                new LongTokenRange(9, 10)
        );

        LongTokenRange tokenRange = new LongTokenRange(0, 10);
        ImmutableSet<Node> replicas = ImmutableSet.of(mock(Node.class), mock(Node.class));
        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(tokenRange);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(ImmutableList.of(new VnodeRepairState(tokenRange, replicas, 1234L))).build();
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, vnodes);

        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroups(Collections.singletonList(replicaRepairGroup))
                .withLastRepairedAt(1234L)
                .withVnodeRepairStates(vnodeRepairStates)
                .build();
        when(myRepairState.getSnapshot()).thenReturn(repairStateSnapshot);
        // 100 MB target size, 1000MB in table
        when(myTableStorageStates.getDataSize(eq(myTableReference))).thenReturn(THOUSAND_MB_IN_BYTES);

        Iterator<ScheduledTask> iterator = myRepairJob.iterator();

        ScheduledTask task = iterator.next();
        assertThat(task).isInstanceOf(RepairGroup.class);
        Collection<RepairTask> repairTasks = ((RepairGroup)task).getRepairTasks();

        assertThat(repairTasks).hasSize(expectedTokenRanges.size());

        Iterator<RepairTask> repairTaskIterator = repairTasks.iterator();
        for (LongTokenRange expectedRange : expectedTokenRanges)
        {
            assertThat(repairTaskIterator.hasNext()).isTrue();
            RepairTask repairTask = repairTaskIterator.next();
            assertThat(repairTask.getReplicas()).containsExactlyInAnyOrderElementsOf(replicas);
            assertThat(repairTask.getRepairConfiguration()).isEqualTo(myRepairConfiguration);
            assertThat(repairTask.getTableReference()).isEqualTo(myTableReference);

            assertThat(repairTask.getTokenRanges()).containsExactly(expectedRange);
        }
    }

    @Test
    public void testStatusCompleted()
    {
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();
        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAt);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);

        assertThat(myRepairJob.getView().getStatus()).isEqualTo(RepairJobView.Status.COMPLETED);
    }

    @Test
    public void testStatusError()
    {
        long repairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAt);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getStatus()).isEqualTo(RepairJobView.Status.ERROR);
    }

    @Test
    public void testStatusInQueue()
    {
        long repairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAt);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getStatus()).isEqualTo(RepairJobView.Status.IN_QUEUE);
    }

    @Test
    public void testStatusWarning()
    {
        long repairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAt);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getStatus()).isEqualTo(RepairJobView.Status.WARNING);
    }


    @Test
    public void testHalfCompleteProgress()
    {
        long repairedAtFirst = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        long lastRepairedAtSecond = System.currentTimeMillis();
        long lastRepairedAtThird = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(9);

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAtThird);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);

        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, vnodeRepairState2)).build();
        doReturn(vnodeRepairStates).when(myRepairStateSnapshot).getVnodeRepairStates();
        doReturn(repairedAtFirst).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getProgress()).isEqualTo(0.5d);
    }

    @Test
    public void testCompletedProgress()
    {
        long repairedAt = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAt);

        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build();
        doReturn(vnodeRepairStates).when(myRepairStateSnapshot).getVnodeRepairStates();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getProgress()).isEqualTo(1);
    }

    @Test
    public void testInQueueProgress()
    {
        long repairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
        long repairedAtSecond = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(4);

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), repairedAtSecond);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), repairedAt);

        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, vnodeRepairState2)).build();
        doReturn(vnodeRepairStates).when(myRepairStateSnapshot).getVnodeRepairStates();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();

        assertThat(myRepairJob.getView().getProgress()).isEqualTo(0);
    }

}
