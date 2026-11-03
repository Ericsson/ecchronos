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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
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
public class TestUnifiedIncrementalRepairJob
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
    private ReplicationState myReplicationState;
    @Mock
    private CassandraMetrics myCassandraMetrics;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;
    @Mock
    private Node myNode1;
    @Mock
    private Node myNode2;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myNode1.getHostId()).thenReturn(myNodeId1);
        when(myNode2.getHostId()).thenReturn(myNodeId2);
        doReturn(Collections.emptyIterator())
                .when(myRepairHistoryProvider).iterate(any(Node.class), eq(myTableReference), anyLong(), anyLong(),
                        any());
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.UNIFIED_INCREMENTAL)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS)
                .withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .build();
    }

    @Test
    public void testGetViewsReturnsOneViewPerNodeSharingJobId()
    {
        // Nothing repaired on either node -> both delegates fully behind.
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(any(UUID.class), eq(myTableReference));
        doReturn(System.currentTimeMillis()).when(myCassandraMetrics).getMaxRepairedAt(any(UUID.class),
                eq(myTableReference));

        UnifiedIncrementalRepairJob job = buildJob();

        List<ScheduledRepairJobView> views = job.getViews();
        assertThat(views).hasSize(2);
        assertThat(views).extracting(ScheduledRepairJobView::getNodeId)
                .containsExactlyInAnyOrder(myNodeId1, myNodeId2);
        // All per-node views carry the consolidated job's id (per-(job,node) identity consistent with vnode).
        assertThat(views).extracting(ScheduledRepairJobView::getJobId)
                .containsExactly(job.getJobId(), job.getJobId());
        assertThat(views).extracting(ScheduledRepairJobView::getRepairType)
                .containsOnly(RepairType.UNIFIED_INCREMENTAL);
        assertThat(job.getNodeIds()).containsExactlyInAnyOrder(myNodeId1, myNodeId2);
    }

    @Test
    public void testPriorityIsMaxUrgencyAcrossNodes()
    {
        // node1 recently repaired (not runnable), node2 overdue (runnable). Priority must reflect node2.
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(eq(myNodeId1), eq(myTableReference));
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(eq(myNodeId2), eq(myTableReference));
        doReturn(System.currentTimeMillis()).when(myCassandraMetrics).getMaxRepairedAt(eq(myNodeId1),
                eq(myTableReference));
        doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS))
                .when(myCassandraMetrics).getMaxRepairedAt(eq(myNodeId2), eq(myTableReference));

        UnifiedIncrementalRepairJob job = buildJob();

        assertThat(job.getRealPriority()).isGreaterThan(0);
        // The most-overdue node (node2) drives the representative view.
        assertThat(job.getView().getNodeId()).isEqualTo(myNodeId2);
    }

    @Test
    public void testNotRunnableWhenAllNodesFullyRepaired()
    {
        doReturn(100.0d).when(myCassandraMetrics).getPercentRepaired(any(UUID.class), eq(myTableReference));

        UnifiedIncrementalRepairJob job = buildJob();
        job.refreshState();

        assertThat(job.runnable()).isFalse();
        assertThat(job.getRealPriority()).isEqualTo(-1);
    }

    @Test
    public void testRunnableWhenAnyNodeHasPendingData()
    {
        doReturn(100.0d).when(myCassandraMetrics).getPercentRepaired(eq(myNodeId1), eq(myTableReference));
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(eq(myNodeId2), eq(myTableReference));
        doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS))
                .when(myCassandraMetrics).getMaxRepairedAt(eq(myNodeId2), eq(myTableReference));

        UnifiedIncrementalRepairJob job = buildJob();
        job.refreshState();

        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testIteratorEmitsIncrementalTaskPerRunnableNode()
    {
        // Both nodes overdue -> both produce a task.
        DriverNode replica = org.mockito.Mockito.mock(DriverNode.class);
        doReturn(myNodeId1).when(replica).getId();
        ImmutableSet<DriverNode> replicas = ImmutableSet.of(replica);
        doReturn(replicas).when(myReplicationState).getReplicas(eq(myTableReference), any(Node.class));
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(any(UUID.class), eq(myTableReference));
        doReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS))
                .when(myCassandraMetrics).getMaxRepairedAt(any(UUID.class), eq(myTableReference));
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);

        UnifiedIncrementalRepairJob job = buildJob();

        int taskCount = 0;
        java.util.Iterator<ScheduledTask> iterator = job.iterator();
        while (iterator.hasNext())
        {
            iterator.next();
            taskCount++;
        }
        assertThat(taskCount).isEqualTo(2);
    }

    @Test
    public void testPostExecuteRoutesHistoryWriteToOwningNode()
    {
        DriverNode replica = org.mockito.Mockito.mock(DriverNode.class);
        doReturn(myNodeId1).when(replica).getId();
        doReturn(ImmutableSet.of(replica)).when(myReplicationState).getReplicas(eq(myTableReference), any(Node.class));
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(any(UUID.class), eq(myTableReference));

        UnifiedIncrementalRepairJob job = buildJob();

        // A task bound to node1.
        ScheduledTask node1Task = new ScheduledTask()
        {
            @Override
            public UUID getNodeId()
            {
                return myNodeId1;
            }

            @Override
            public TaskExecutionResult execute(final UUID nodeID)
            {
                return TaskExecutionResult.SUCCESS;
            }
        };

        job.postExecute(TaskExecutionResult.SUCCESS, node1Task);

        // The owning node1 delegate must record completed repair history exactly once.
        verify(myRepairHistory).recordCompletedRepair(eq(myTableReference), any(UUID.class), any(), any(),
                eq(RepairType.UNIFIED_INCREMENTAL), anyLong(), anyLong(), eq(RepairStatus.SUCCESS));
    }

    private UnifiedIncrementalRepairJob buildJob()
    {
        return new UnifiedIncrementalRepairJob.Builder()
                .withConfiguration(new ScheduledJob.ConfigurationBuilder()
                        .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS).build())
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withRepairLockType(RepairLockType.VNODE)
                .withNodeState(new IncrementalNodeState(myNode1, delegate(myNode1)))
                .withNodeState(new IncrementalNodeState(myNode2, delegate(myNode2)))
                .build();
    }

    private IncrementalRepairJob delegate(final Node node)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS).build();
        return new IncrementalRepairJob.Builder()
                .withConfiguration(configuration)
                .withNode(node)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withRepairConfiguration(myRepairConfiguration)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withCassandraMetrics(myCassandraMetrics)
                .withReplicationState(myReplicationState)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .build();
    }
}
