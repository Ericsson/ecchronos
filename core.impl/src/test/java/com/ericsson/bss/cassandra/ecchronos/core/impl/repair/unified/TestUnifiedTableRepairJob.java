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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestUnifiedTableRepairJob
{
    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long WARNING_IN_DAYS = 7;
    private static final long ERROR_IN_DAYS = 10;

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId1 = UUID.randomUUID();
    private final UUID myNodeId2 = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
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
    @Mock
    private RepairState myRepairState1;
    @Mock
    private RepairState myRepairState2;
    @Mock
    private RepairStateSnapshot mySnapshot1;
    @Mock
    private RepairStateSnapshot mySnapshot2;
    @Mock
    private VnodeRepairStates myVnodeRepairStates;

    @Before
    public void setup()
    {
        when(myNode1.getHostId()).thenReturn(myNodeId1);
        when(myNode2.getHostId()).thenReturn(myNodeId2);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairState1.getSnapshot()).thenReturn(mySnapshot1);
        when(myRepairState2.getSnapshot()).thenReturn(mySnapshot2);
        when(mySnapshot1.getVnodeRepairStates()).thenReturn(myVnodeRepairStates);
        when(mySnapshot2.getVnodeRepairStates()).thenReturn(myVnodeRepairStates);
        when(myVnodeRepairStates.getVnodeRepairStates()).thenReturn(Collections.emptyList());
    }

    @Test
    public void testGetViewsReturnsOneViewPerNode()
    {
        when(mySnapshot1.lastCompletedAt()).thenReturn(System.currentTimeMillis());
        when(mySnapshot2.lastCompletedAt()).thenReturn(System.currentTimeMillis());

        UnifiedTableRepairJob job = buildJob();

        List<ScheduledRepairJobView> views = job.getViews();
        assertThat(views).hasSize(2);
        assertThat(views).extracting(ScheduledRepairJobView::getNodeId)
                .containsExactlyInAnyOrder(myNodeId1, myNodeId2);
        // No node can repair here (canRepair() defaults to false), so getView() falls back to the first node.
        assertThat(job.getView().getNodeId()).isEqualTo(myNodeId1);
        assertThat(job.getNodeIds()).containsExactlyInAnyOrder(myNodeId1, myNodeId2);
    }

    @Test
    public void testGetViewReturnsMostOverdueRepairableNode()
    {
        long now = System.currentTimeMillis();
        // Both nodes can repair; node2 is the most overdue -> getView() must represent node2.
        when(mySnapshot1.canRepair()).thenReturn(true);
        when(mySnapshot2.canRepair()).thenReturn(true);
        when(mySnapshot1.lastCompletedAt()).thenReturn(now);
        when(mySnapshot2.lastCompletedAt()).thenReturn(now - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS));

        UnifiedTableRepairJob job = buildJob();

        assertThat(job.getView().getNodeId()).isEqualTo(myNodeId2);
    }

    @Test
    public void testPriorityIsMaxUrgencyAcrossNodes()
    {
        // Node1 recently repaired; node2 very overdue. Priority must reflect node2 (the most overdue).
        long now = System.currentTimeMillis();
        long overdue = now - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS);
        when(mySnapshot1.canRepair()).thenReturn(true);
        when(mySnapshot2.canRepair()).thenReturn(true);
        when(mySnapshot1.getRepairGroups())
                .thenReturn(ImmutableList.of(new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of(), now)));
        when(mySnapshot2.getRepairGroups())
                .thenReturn(ImmutableList.of(new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of(), overdue)));

        UnifiedTableRepairJob job = buildJob();

        int priorityBoth = job.getRealPriority();

        // Compare against a job where only the recently-repaired node1 exists: the two-node job (with the
        // overdue node2) must have strictly higher priority.
        when(mySnapshot1.getRepairGroups())
                .thenReturn(ImmutableList.of(new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of(), now)));
        UnifiedTableRepairJob singleNodeRecent = buildJobSingleNode();
        int prioritySingleRecent = singleNodeRecent.getRealPriority();

        assertThat(priorityBoth).isGreaterThan(prioritySingleRecent);
    }

    @Test
    public void testNotRunnableWhenNoNodeCanRepair()
    {
        when(mySnapshot1.canRepair()).thenReturn(false);
        when(mySnapshot2.canRepair()).thenReturn(false);
        when(mySnapshot1.lastCompletedAt()).thenReturn(System.currentTimeMillis());
        when(mySnapshot2.lastCompletedAt()).thenReturn(System.currentTimeMillis());

        UnifiedTableRepairJob job = buildJob();

        assertThat(job.runnable()).isFalse();
    }

    private UnifiedTableRepairJob buildJob()
    {
        return baseBuilder()
                .withNodeRepairState(new NodeRepairState(myNode1, myRepairState1))
                .withNodeRepairState(new NodeRepairState(myNode2, myRepairState2))
                .build();
    }

    private UnifiedTableRepairJob buildJobSingleNode()
    {
        return baseBuilder()
                .withNodeRepairState(new NodeRepairState(myNode1, myRepairState1))
                .build();
    }

    private UnifiedTableRepairJob.Builder baseBuilder()
    {
        RepairConfiguration config = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.UNIFIED_VNODE)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS)
                .withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .build();
        return new UnifiedTableRepairJob.Builder()
                .withConfiguration(new ScheduledJob.ConfigurationBuilder()
                        .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS).build())
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(config)
                .withTableStorageStates(myTableStorageStates)
                .withRepairHistory(myRepairHistory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy);
    }
}
