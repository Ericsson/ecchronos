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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.MockedClock;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;

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
    private TableMetadata myTableMetadata;

    @Mock
    private TableOptionsMetadata myTableOptionsMetadata;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairFaultReporter myFaultReporter;

    @Mock
    private TableStorageStates myTableStorageStates;


    private TableRepairJob myRepairJob;

    private MockedClock myClock = new MockedClock();

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);
    private RepairConfiguration myRepairConfiguration;

    @Before
    public void startup()
    {
        doReturn(-1L).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(myRepairStateSnapshot).when(myRepairState).getSnapshot();

        doNothing().when(myRepairState).update();

        doReturn(myTableOptionsMetadata).when(myTableMetadata).getOptions();
        doReturn(myTableMetadata).when(myKeyspaceMetadata).getTable(eq(tableName));
        doReturn(myKeyspaceMetadata).when(myMetadata).getKeyspace(eq(keyspaceName));

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .withTargetRepairSizeInBytes(HUNDRED_MB_IN_BYTES)
                .build();

        myRepairJob = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRepairState(myRepairState)
                .withFaultReporter(myFaultReporter)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(myTableStorageStates)
                .build();

        myRepairJob.setClock(myClock);
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

        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNeedRepair()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(0L);
        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairStateSnapshot, times(2)).canRepair();
    }

    @Test
    public void testPrevalidateNotRepairableThenRepairable()
    {
        // mock
        doReturn(false).doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(0L);
        assertThat(myRepairJob.runnable()).isFalse();
        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairStateSnapshot, times(3)).canRepair();
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

        myRepairJob.postExecute(true);

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

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepaired()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepairedWithFailure()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteUpdateThrowsException()
    {
        // mock
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        myRepairJob.runnable();

        // verify alarm ceased in preValidate
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
        reset(myFaultReporter);

        myRepairJob.postExecute(true);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isFalse();

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        myRepairJob.postExecute(true);

        // verify - repaired
        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        myClock.setTime(start);

        myRepairJob.runnable();

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatAlarmIsNotSentWhenGcGraceIsBelowRepairInterval()
    {
        // setup - not repaired
        int tableGcGrace = (int) TimeUnit.HOURS.toSeconds(22);
        long hoursSinceLastRepair = 23;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.HOURS.toMillis(hoursSinceLastRepair);

        // mock - not repaired
        doReturn(tableGcGrace).when(myTableOptionsMetadata).getGcGraceInSeconds();
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.runnable()).isFalse();

        // verify - not repaired
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    @Test
    public void testLastSuccessfulRunIsBasedOnRepairHistory()
    {
        long timeOffset = TimeUnit.MINUTES.toMillis(1);
        long now = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2);
        long lastRepairedAtWarning = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS * 2);
        long lastRepairedAtAfterRepair = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + timeOffset;

        myClock.setTime(now);

        // We have waited 2 days to repair, send alarm and run repair
        doReturn(lastRepairedAtWarning).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepairedAtWarning);

        assertThat(myRepairJob.runnable()).isTrue();

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtWarning);
        verify(myFaultReporter).raise(eq(FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));
        verifyNoMoreInteractions(myFaultReporter);
        reset(myFaultReporter);

        // Repair has been completed
        doReturn(lastRepairedAtAfterRepair).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepairedAtAfterRepair);

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtAfterRepair);
        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));

        // After 10 ms we can repair again
        myClock.setTime(now + timeOffset);
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isTrue();

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtAfterRepair);
        verify(myFaultReporter, times(0)).raise(any(FaultCode.class), anyMapOf(String.class, Object.class));
    }

    @Test
    public void testGetView()
    {
        RepairJobView repairJobView = myRepairJob.getView();

        assertThat(repairJobView.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJobView.getRepairConfiguration()).isEqualTo(myRepairConfiguration);
        assertThat(repairJobView.getRepairStateSnapshot()).isEqualTo(myRepairStateSnapshot);
    }

    @Test
    public void testIterator()
    {
        LongTokenRange tokenRange = new LongTokenRange(0, 10);
        ImmutableSet<Host> replicas = ImmutableSet.of(mock(Host.class), mock(Host.class));
        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(tokenRange);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl
                .newBuilder(ImmutableList.of(new VnodeRepairState(tokenRange, replicas, 1234L)))
                .build();
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, vnodes, System.currentTimeMillis());

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
        ImmutableSet<Host> replicas = ImmutableSet.of(mock(Host.class), mock(Host.class));
        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(tokenRange);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(ImmutableList.of(new VnodeRepairState(tokenRange, replicas, 1234L))).build();
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, vnodes, System.currentTimeMillis());

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

    public void testGetRealPriority()
    {
        long lastRepaired = System.currentTimeMillis();
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(-1);

        lastRepaired = System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) - TimeUnit.HOURS.toMillis(1));
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(-1);

        lastRepaired = System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(1);

        lastRepaired = System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + TimeUnit.HOURS.toMillis(1));
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        mockRepairGroup(lastRepaired);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(2);
    }

    @Test
    public void testGetRealPrioritySnapshotLastRepairedAtLowerThanRepairGroups()
    {
        long lastRepairedAtSnapshot = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(14);
        doReturn(lastRepairedAtSnapshot).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(true).when(myRepairStateSnapshot).canRepair();
        long firstRepairGroupLastRepairedAt = System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        ReplicaRepairGroup firstReplicaRepairGroup = getRepairGroup(new LongTokenRange(1, 2), firstRepairGroupLastRepairedAt);
        mockRepairGroup(firstReplicaRepairGroup);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(1);

        long secondRepairGroupLastRepairedAt = System.currentTimeMillis() - (TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + TimeUnit.HOURS.toMillis(1));
        ReplicaRepairGroup secondReplicaRepairGroup = getRepairGroup(new LongTokenRange(2, 3), secondRepairGroupLastRepairedAt);
        mockRepairGroup(secondReplicaRepairGroup, firstReplicaRepairGroup);
        assertThat(myRepairJob.getRealPriority()).isEqualTo(2);
    }

    private void mockRepairGroup(long lastRepairedAt)
    {
        mockRepairGroup(getRepairGroup(new LongTokenRange(1, 2), lastRepairedAt));
    }

    private void mockRepairGroup(ReplicaRepairGroup ...replicaRepairGroups)
    {
        List<ReplicaRepairGroup> repairGroups = new ArrayList<>();
        for (ReplicaRepairGroup replicaRepairGroup : replicaRepairGroups)
        {
            repairGroups.add(replicaRepairGroup);
        }
        when(myRepairStateSnapshot.getRepairGroups()).thenReturn(repairGroups);
    }

    private ReplicaRepairGroup getRepairGroup(LongTokenRange range, long lastRepairedAt)
    {
        ImmutableSet<Host> replicas = ImmutableSet.of(mock(Host.class), mock(Host.class));
        return new ReplicaRepairGroup(replicas, ImmutableList.of(range), lastRepairedAt);
    }
}
