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
import com.datastax.driver.core.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith (MockitoJUnitRunner.class)
public class TestRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table");
    private static final TableReference TABLE_REFERENCE2 = tableReference("keyspace", "table2");
    private static final VnodeRepairState VNODE_REPAIR_STATE = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairStateFactory myRepairStateFactory;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private Metadata myMetadata;

    @Mock
    private RepairStatus myRepairStatus;

    @Before
    public void init()
    {
        when(myRepairState.getSnapshot()).thenReturn(myRepairStateSnapshot);
        when(myRepairStateFactory.create(eq(TABLE_REFERENCE), any(), any())).thenReturn(myRepairState);
        when(myRepairStateFactory.create(eq(TABLE_REFERENCE2), any(), any())).thenReturn(myRepairState);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(VNODE_REPAIR_STATE)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
    }

    @Test
    public void testConfigureNewTable()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testConfigureTwoTables()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);
        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE2, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000).times(2)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE2), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));

        repairSchedulerImpl.close();
        verify(scheduleManager, times(2)).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRemoveTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.removeConfiguration(TABLE_REFERENCE);
        verify(scheduleManager, timeout(1000)).deschedule(any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        long expectedUpdatedRepairInterval = TimeUnit.DAYS.toMillis(1);

        RepairConfiguration updatedRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(expectedUpdatedRepairInterval, TimeUnit.MILLISECONDS)
                .build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, updatedRepairConfiguration);

        verify(scheduleManager, timeout(1000).times(2)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, timeout(1000)).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(updatedRepairConfiguration), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, updatedRepairConfiguration);

        repairSchedulerImpl.close();
        verify(scheduleManager, times(2)).deschedule(any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfigurationToSame()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getCurrentRepairJobs()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableAbortedRepairExists()
    {
        Set<OngoingRepair> ongoingRepairs = new HashSet<>();
        UUID repairId = UUID.randomUUID();
        OngoingRepair.Status status = OngoingRepair.Status.started;
        Long startedAt = System.currentTimeMillis();
        String triggeredBy = TABLE_REFERENCE.getId().toString();
        int remainingTasks = 3;
        OngoingRepair ongoingRepair = new OngoingRepair.Builder()
                .withOngoingRepairInfo(repairId, status, -1L, startedAt, triggeredBy, remainingTasks)
                .build();
        ongoingRepairs.add(ongoingRepair);
        when(myRepairStatus.getUnfinishedRepairs()).thenReturn(ongoingRepairs);
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT), any());
        verify(myRepairState, atLeastOnce()).update(startedAt);
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DEFAULT);
        Optional<ScheduleView> schedule = repairSchedulerImpl.getSchedules().stream().findFirst();
        assertThat(schedule).isNotEmpty();

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getSchedules()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleOnDemandRepair()
    {
        KeyspaceMetadata ksMetadata = mock(KeyspaceMetadata.class);
        when(myMetadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(ksMetadata);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(ksMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(tableMetadata);
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.scheduleOnDemandRepair(TABLE_REFERENCE);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DISABLED), any());
        verify(myRepairState, atLeastOnce()).update(any(long.class));
        assertOneTableViewExist(repairSchedulerImpl, TABLE_REFERENCE, RepairConfiguration.DISABLED);

        repairSchedulerImpl.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));
        assertThat(repairSchedulerImpl.getSchedules()).isEmpty();

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    private void assertOneTableViewExist(RepairScheduler repairScheduler, TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        List<RepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();
        assertThat(repairJobViews).hasSize(1);

        RepairJobView repairJobView = repairJobViews.get(0);
        assertThat(repairJobView.getTableReference()).isEqualTo(tableReference);
        assertThat(repairJobView.getRepairConfiguration()).isEqualTo(repairConfiguration);
    }

    private RepairSchedulerImpl.Builder defaultRepairSchedulerImplBuilder()
    {
        return RepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withRepairStateFactory(myRepairStateFactory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(myTableStorageStates)
                .withRepairHistory(myRepairHistory)
                .withMetadata(myMetadata)
                .withRepairStatus(myRepairStatus);
    }
}
