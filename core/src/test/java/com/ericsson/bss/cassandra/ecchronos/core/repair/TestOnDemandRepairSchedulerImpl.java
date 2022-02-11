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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.PostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table");

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState replicationState;

    @Mock
    private RepairHistory repairHistory;

    @Mock
    private Metadata metadata;

    @Mock
    private OnDemandStatus myOnDemandStatus;

    @Mock
    private Metadata myMetadata;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableMetadata myTableMetadata;

    @Mock
    private OngoingJob myOngingJob;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private RepairStateFactory myRepairStateFactory;

    @Mock
    private RepairState myRepairState;

    @Test
    public void testScheduleRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(myTableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(myTableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        RepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager, times(2)).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartRepairOnTable() throws EcChronosException
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenReturn(ongoingJobs);

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartRepairOnTableWithException() throws EcChronosException
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        Map<EndPoint, Throwable> errors = new HashMap<>();
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenThrow(new NoHostAvailableException(errors )).thenReturn(ongoingJobs);

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(15000)).schedule(any(ScheduledJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNull() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(null);
    }

    private void assertTableViewExist(OnDemandRepairSchedulerImpl repairScheduler, RepairJobView... expectedViews)
    {
        List<RepairJobView> repairJobViews = repairScheduler.getActiveRepairJobs();
        assertThat(repairJobViews).containsExactlyInAnyOrder(expectedViews);
    }

    private OnDemandRepairSchedulerImpl.Builder defaultOnDemandRepairSchedulerImplBuilder()
    {
        when(myRepairStateFactory.create(any(), any(), any())).thenReturn(myRepairState);
        mockRepairStateSnapshot(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10));
        return OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withReplicationState(replicationState)
                .withMetadata(metadata)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairHistory(repairHistory)
                .withOnDemandStatus(myOnDemandStatus)
                .withRepairStateFactory(myRepairStateFactory)
                .withTableStorageStates(myTableStorageStates);
    }

    private void mockRepairStateSnapshot(long lastRepairedAt)
    {
        LongTokenRange tokenRange = new LongTokenRange(0, 10);
        ImmutableSet<Node> replicas = ImmutableSet.of(mock(Node.class), mock(Node.class));
        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(tokenRange);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl
                .newBuilder(ImmutableList.of(new VnodeRepairState(tokenRange, replicas, lastRepairedAt)))
                .build();
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, vnodes);

        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroups(Collections.singletonList(replicaRepairGroup))
                .withLastCompletedAt(lastRepairedAt)
                .withVnodeRepairStates(vnodeRepairStates)
                .build();
        when(myRepairState.getSnapshot()).thenReturn(repairStateSnapshot);
    }
}
