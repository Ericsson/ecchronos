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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestUnifiedRepairScheduler
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table1");
    private static final RepairConfiguration UNIFIED_CONFIG = RepairConfiguration.newBuilder()
            .withRepairType(RepairType.UNIFIED_VNODE).build();
    private static final VnodeRepairState VNODE_REPAIR_STATE =
            TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private ScheduleManager myScheduleManager;
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
    private RepairHistoryService myRepairHistory;
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;
    @Mock
    private Node myNode1;
    @Mock
    private Node myNode2;

    private final UUID myNodeId1 = UUID.randomUUID();
    private final UUID myNodeId2 = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(myNode1.getHostId()).thenReturn(myNodeId1);
        when(myNode2.getHostId()).thenReturn(myNodeId2);
        when(myRepairState.getSnapshot()).thenReturn(myRepairStateSnapshot);
        when(myRepairStateFactory.create(any(), eq(TABLE_REFERENCE), any(), any())).thenReturn(myRepairState);
        VnodeRepairStatesImpl vnodeRepairStates =
                VnodeRepairStatesImpl.newBuilder(Arrays.asList(VNODE_REPAIR_STATE)).build();
        when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
    }

    @Test
    public void testSingleNodeCreatesOneJobWithOneView()
    {
        UnifiedRepairScheduler scheduler = defaultBuilder().build();

        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));

        verify(myScheduleManager, timeout(1000)).schedule(eq(myNodeId1), any(ScheduledJob.class));
        verify(myRepairStateFactory, timeout(1000)).create(eq(myNode1), eq(TABLE_REFERENCE), eq(UNIFIED_CONFIG), any());

        List<ScheduledRepairJobView> views = scheduler.getCurrentRepairJobs();
        assertThat(views).hasSize(1);
        assertThat(views.get(0).getTableReference()).isEqualTo(TABLE_REFERENCE);

        scheduler.close();
    }

    @Test
    public void testTwoNodesConsolidateIntoOneJobWithTwoViews()
    {
        UnifiedRepairScheduler scheduler = defaultBuilder().build();

        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        verify(myScheduleManager, timeout(1000)).schedule(any(UUID.class), any(ScheduledJob.class));

        // Second node reporting the same table rebuilds the single consolidated job (deschedule old, schedule new).
        scheduler.putConfigurations(myNode2, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        verify(myScheduleManager, timeout(1000)).deschedule(any(UUID.class), any(ScheduledJob.class));
        verify(myScheduleManager, timeout(1000).times(2)).schedule(any(UUID.class), any(ScheduledJob.class));

        // One job, but two per-node views.
        List<ScheduledRepairJobView> views = scheduler.getCurrentRepairJobs();
        assertThat(views).hasSize(2);
        assertThat(views).extracting(ScheduledRepairJobView::getNodeId)
                .containsExactlyInAnyOrder(myNodeId1, myNodeId2);

        scheduler.close();
    }

    @Test
    public void testGetCurrentRepairJobsByNodeFiltersToThatNode()
    {
        UnifiedRepairScheduler scheduler = defaultBuilder().build();

        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        scheduler.putConfigurations(myNode2, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        verify(myScheduleManager, timeout(1000).times(2)).schedule(any(UUID.class), any(ScheduledJob.class));

        List<ScheduledRepairJobView> node1Views = scheduler.getCurrentRepairJobsByNode(myNodeId1);
        assertThat(node1Views).hasSize(1);
        assertThat(node1Views.get(0).getNodeId()).isEqualTo(myNodeId1);

        scheduler.close();
    }

    @Test
    public void testRemoveConfigurationDeschedulesJob()
    {
        UnifiedRepairScheduler scheduler = defaultBuilder().build();

        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        verify(myScheduleManager, timeout(1000)).schedule(eq(myNodeId1), any(ScheduledJob.class));

        scheduler.removeConfiguration(myNode1, TABLE_REFERENCE);
        verify(myScheduleManager, timeout(1000)).deschedule(any(UUID.class), any(ScheduledJob.class));
        assertThat(scheduler.getCurrentRepairJobs()).isEmpty();

        scheduler.close();
    }

    @Test
    public void testRepeatedSameConfigDoesNotRebuild()
    {
        UnifiedRepairScheduler scheduler = defaultBuilder().build();

        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));
        verify(myScheduleManager, timeout(1000)).schedule(eq(myNodeId1), any(ScheduledJob.class));

        // Same node, same config -> no rebuild (no extra schedule, no deschedule).
        scheduler.putConfigurations(myNode1, TABLE_REFERENCE, Collections.singleton(UNIFIED_CONFIG));

        verify(myScheduleManager, times(1)).schedule(eq(myNodeId1), any(ScheduledJob.class));
        verify(myScheduleManager, never()).deschedule(any(UUID.class), any(ScheduledJob.class));

        scheduler.close();
    }

    private UnifiedRepairScheduler.Builder defaultBuilder()
    {
        return UnifiedRepairScheduler.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withScheduleManager(myScheduleManager)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairStateFactory(myRepairStateFactory)
                .withTableStorageStates(myTableStorageStates)
                .withRepairHistory(myRepairHistory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy);
    }
}
