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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.IncrementalOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.VnodeOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace1", "table1");

    @Mock
    private DistributedJmxProxyFactory jmxProxyFactory;

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

    @Mock
    private Node myNode;

    private final UUID myNodeId = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(session.getMetadata()).thenReturn(metadata);
        when(myOngingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngingJob.getHostId()).thenReturn(myNodeId);
        when(myOngingJob.getJobId()).thenReturn(UUID.randomUUID());
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myOngingJob.getRepairType()).thenReturn(RepairType.VNODE);
        when(myOnDemandStatus.getNodes()).thenReturn(Arrays.asList(myNode));
    }

    @Test
    public void testScheduleVnodeRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.VNODE, myNode.getHostId());
        verify(scheduleManager).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleIncrementalRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.INCREMENTAL, myNode.getHostId());
        verify(scheduleManager).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoVnodeRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.VNODE, myNode.getHostId());
        OnDemandRepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE,RepairType.VNODE, myNode.getHostId());
        verify(scheduleManager, times(2)).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoIncrementalRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(Optional.of(myTableMetadata));

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));
        OnDemandRepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.INCREMENTAL, myNode.getHostId());
        OnDemandRepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.INCREMENTAL, myNode.getHostId());
        verify(scheduleManager, times(2)).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartVnodeRepairOnTable()
    {
        when(myOngingJob.getRepairType()).thenReturn(RepairType.VNODE);
        Map<UUID, Set<OngoingJob>> allOnGoingJobs = new HashMap<>();
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        allOnGoingJobs.put(myNodeId, ongoingJobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(replicationState)).thenReturn(allOnGoingJobs);
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, timeout(1000)).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartIncrementalRepairOnTable()
    {
        when(myOngingJob.getRepairType()).thenReturn(RepairType.INCREMENTAL);
        when(replicationState.getReplicas(TABLE_REFERENCE, myNode)).thenReturn(ImmutableSet.of(mock(DriverNode.class)));
        Map<UUID, Set<OngoingJob>> allOnGoingJobs = new HashMap<>();
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        allOnGoingJobs.put(myNodeId, ongoingJobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(replicationState)).thenReturn(allOnGoingJobs);
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(1000)).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartVnodeRepairOnTableWithException()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        List<Map.Entry<Node, Throwable>> errors = new ArrayList<>();
        when(myOngingJob.getRepairType()).thenReturn(RepairType.VNODE);
        Map<UUID, Set<OngoingJob>> allOnGoingJobs = new HashMap<>();
        allOnGoingJobs.put(myNodeId, ongoingJobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(replicationState)).thenReturn(allOnGoingJobs);
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(15000)).schedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(VnodeOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartIncrementalRepairOnTableWithException()
    {
        Set<OngoingJob> ongoingJobs = new HashSet<>();
        ongoingJobs.add(myOngingJob);
        List<Map.Entry<Node, Throwable>> errors = new ArrayList<>();
        when(myOngingJob.getRepairType()).thenReturn(RepairType.INCREMENTAL);
        Map<UUID, Set<OngoingJob>> allOnGoingJobs = new HashMap<>();
        allOnGoingJobs.put(myNodeId, ongoingJobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(replicationState)).thenReturn(allOnGoingJobs);
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, timeout(15000)).schedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        repairScheduler.close();
        verify(scheduleManager).deschedule(eq(myNode.getHostId()), any(IncrementalOnDemandRepairJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.VNODE, myNode.getHostId());
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.INCREMENTAL, myNode.getHostId());
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.VNODE, myNode.getHostId());
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(Optional.of(myKeyspaceMetadata));
        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE, RepairType.INCREMENTAL, myNode.getHostId());
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleVnodeRepairOnNull() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(null, RepairType.VNODE, myNode.getHostId());
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleIncrementalRepairOnNull() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        verify(scheduleManager, never()).schedule(eq(myNode.getHostId()), any(ScheduledJob.class));
        repairScheduler.scheduleJob(null, RepairType.INCREMENTAL, myNode.getHostId());
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

