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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace1", "table1");

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
    private CqlSession session;

    @Mock
    private OnDemandStatus myOnDemandStatus;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableMetadata myTableMetadata;

    @Mock
    private OngoingJob myOngingJob;

    @Before
    public void setup()
    {
        when(session.getMetadata()).thenReturn(metadata);
        when(myOngingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngingJob.getJobId()).thenReturn(UUID.randomUUID());
    }

    @Test
    public void testScheduleVnodeRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(any(VnodeOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, false);
        verify(scheduleManager).schedule(any(VnodeOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleIncrementalRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(any(IncrementalOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, true);
        verify(scheduleManager).schedule(any(IncrementalOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoVnodeRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(any(VnodeOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, false);
        OnDemandRepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE, false);
        verify(scheduleManager, times(2)).schedule(any(VnodeOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoIncrementalRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(any(IncrementalOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, true);
        OnDemandRepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE, true);
        verify(scheduleManager, times(2)).schedule(any(IncrementalOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartVnodeRepairOnTable()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        when(myOngingJob.getRepairType()).thenReturn(RepairOptions.RepairType.VNODE);
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenReturn(ongoingJobs);

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(1000)).schedule(any(VnodeOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartIncrementalRepairOnTable()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        when(myOngingJob.getRepairType()).thenReturn(RepairOptions.RepairType.INCREMENTAL);
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenReturn(ongoingJobs);
        when(replicationState.getReplicas(TABLE_REFERENCE)).thenReturn(ImmutableSet.of(mock(DriverNode.class)));

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(1000)).schedule(any(IncrementalOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartVnodeRepairOnTableWithException()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        List<Map.Entry<Node, Throwable>> errors = new ArrayList<>();
        when(myOngingJob.getRepairType()).thenReturn(RepairOptions.RepairType.VNODE);
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenThrow(AllNodesFailedException.fromErrors(errors)).thenReturn(ongoingJobs);

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(15000)).schedule(any(VnodeOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartIncrementalRepairOnTableWithException()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        List<Map.Entry<Node, Throwable>> errors = new ArrayList<>();
        when(myOngingJob.getRepairType()).thenReturn(RepairOptions.RepairType.INCREMENTAL);
        when(myOnDemandStatus.getOngoingJobs(replicationState)).thenThrow(AllNodesFailedException.fromErrors(errors)).thenReturn(ongoingJobs);

        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(15000)).schedule(any(IncrementalOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, false);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, true);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, false);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, true);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNull() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(null, false);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNull() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(null, true);
    }

    private void assertTableViewExist(OnDemandRepairSchedulerImpl repairScheduler, OnDemandRepairJobView... expectedViews)
    {
        List<OnDemandRepairJobView> repairJobViews = repairScheduler.getActiveRepairJobs();
        assertThat(repairJobViews).containsExactlyInAnyOrder(expectedViews);
    }

    private OnDemandRepairSchedulerImpl.Builder defaultOnDemandRepairSchedulerImplBuilder()
    {
        return OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withReplicationState(replicationState)
                .withSession(session)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairHistory(repairHistory)
                .withOnDemandStatus(myOnDemandStatus);
    }
}
