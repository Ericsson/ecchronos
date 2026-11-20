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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Pins which lock set each job class hands to its tasks, for every {@link RepairLockType}. Vnode jobs follow the
 * configured lock type; incremental jobs use their own fixed lock set and ignore it. This must not change as a side
 * effect of refactoring how repair types are dispatched; see issue #1989.
 * <p>
 * {@link RepairResource#equals(Object)} ignores the slot count, so resources are compared by datacenter, name and
 * slots through {@link #describe(RepairResource)}.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairTypeLockSelection
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table");
    private static final String INCREMENTAL_TABLE_LOCK = "global/RepairResource-keyspace.table-1/1";

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private CassandraMetrics myCassandraMetrics;
    @Mock
    private RepairHistoryService myRepairHistoryService;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;
    @Mock
    private Node myNode;
    @Mock
    private OngoingJob myOngoingJob;
    @Mock
    private RepairState myRepairState;
    @Mock
    private RepairStateSnapshot mySnapshot;
    @Mock
    private VnodeRepairStates myVnodeRepairStates;

    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myReplicaIdA = UUID.randomUUID();
    private final UUID myReplicaIdB = UUID.randomUUID();
    private final UUID myReplicaIdC = UUID.randomUUID();
    private DriverNode myReplicaA;
    private DriverNode myReplicaB;
    private DriverNode myReplicaC;

    @Before
    public void setup()
    {
        // Two replicas in DC1 and one in DC2, so the datacenter and node dimensions cannot be confused.
        myReplicaA = mockReplica("DC1", myReplicaIdA);
        myReplicaB = mockReplica("DC1", myReplicaIdB);
        myReplicaC = mockReplica("DC2", myReplicaIdC);

        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        doReturn(0L).when(myCassandraMetrics).getMaxRepairedAt(myNodeId, TABLE_REFERENCE);
        doReturn(ImmutableSet.of(myReplicaA, myReplicaB, myReplicaC))
                .when(myReplicationState).getReplicas(TABLE_REFERENCE, myNode);
        doReturn(Collections.emptyIterator())
                .when(myRepairHistoryProvider).iterate(any(), any(), anyLong(), anyLong(), any());

        when(myRepairState.getSnapshot()).thenReturn(mySnapshot);
        when(mySnapshot.canRepair()).thenReturn(true);
        when(mySnapshot.getVnodeRepairStates()).thenReturn(myVnodeRepairStates);
        when(myVnodeRepairStates.getVnodeRepairStates()).thenReturn(Collections.emptyList());
        when(mySnapshot.getRepairGroups()).thenReturn(List.of(replicaGroup()));
    }

    private static DriverNode mockReplica(final String dataCenter, final UUID id)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(id);
        return node;
    }

    private ReplicaRepairGroup replicaGroup()
    {
        return new ReplicaRepairGroup(ImmutableSet.of(myReplicaA, myReplicaB, myReplicaC), ImmutableList.of(), 0L);
    }

    private static String describe(final RepairResource resource)
    {
        return resource.getDataCenter() + "/" + resource.getResourceName(1) + "/" + resource.getMaxSlots();
    }

    private static Set<String> resourcesOfFirstTask(final Iterator<ScheduledTask> tasks)
    {
        assertThat(tasks.hasNext()).as("job produced a task").isTrue();
        return tasks.next().getRepairResources().stream()
                .map(TestRepairTypeLockSelection::describe)
                .collect(Collectors.toSet());
    }

    private Set<String> nodeLocks()
    {
        return Set.of(
                "DC1/RepairResource-" + myReplicaIdA + "-1/-1",
                "DC1/RepairResource-" + myReplicaIdB + "-1/-1",
                "DC2/RepairResource-" + myReplicaIdC + "-1/-1");
    }

    private static Set<String> dataCenterLocks()
    {
        return Set.of("DC1/RepairResource-DC1-1/-1", "DC2/RepairResource-DC2-1/-1");
    }

    /**
     * The lock set a vnode job is expected to use for the given lock type.
     */
    private Set<String> expectedVnodeLocks(final RepairLockType lockType)
    {
        Set<String> expected = new HashSet<>();
        switch (lockType)
        {
            case DATACENTER:
                expected.addAll(dataCenterLocks());
                break;
            case VNODE:
                expected.addAll(nodeLocks());
                break;
            case DATACENTER_AND_VNODE:
                expected.addAll(dataCenterLocks());
                expected.addAll(nodeLocks());
                break;
            default:
                throw new AssertionError("Lock type " + lockType + " has no pinned expectation, add one");
        }
        return expected;
    }

    /**
     * The fixed lock set incremental jobs use: one single-slot table lock, plus one per-node lock.
     */
    private Set<String> expectedIncrementalLocks()
    {
        Set<String> expected = new HashSet<>(nodeLocks());
        expected.add(INCREMENTAL_TABLE_LOCK);
        return expected;
    }

    private IncrementalRepairJob incrementalJob(final RepairLockType lockType)
    {
        RepairConfiguration configuration = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.INCREMENTAL)
                .build();
        ScheduledJob.Configuration jobConfiguration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(1, TimeUnit.DAYS)
                .build();
        return new IncrementalRepairJob.Builder()
                .withConfiguration(jobConfiguration)
                .withTableReference(TABLE_REFERENCE)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(configuration)
                .withCassandraMetrics(myCassandraMetrics)
                .withNode(myNode)
                .withRepairLockType(lockType)
                .withRepairHistory(myRepairHistory)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .build();
    }

    private TableRepairJob vnodeJob(final RepairLockType lockType, final RepairType repairType)
    {
        RepairConfiguration configuration = RepairConfiguration.newBuilder()
                .withRepairType(repairType)
                .build();
        ScheduledJob.Configuration jobConfiguration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(1, TimeUnit.DAYS)
                .build();
        return new TableRepairJob.Builder()
                .withConfiguration(jobConfiguration)
                .withNode(myNode)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(TABLE_REFERENCE)
                .withRepairState(myRepairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(configuration)
                .withRepairHistory(myRepairHistoryService)
                .withRepairLockType(lockType)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build();
    }

    private void stubOngoingJob(final RepairType repairType)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokens = new HashMap<>();
        tokens.put(new LongTokenRange(1, 2), ImmutableSet.of(myReplicaA, myReplicaB, myReplicaC));
        when(myOngoingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngoingJob.getJobId()).thenReturn(UUID.randomUUID());
        when(myOngoingJob.getHostId()).thenReturn(myNodeId);
        when(myOngoingJob.getRepairType()).thenReturn(repairType);
        when(myOngoingJob.getTokens()).thenReturn(tokens);
        when(myOngoingJob.getRepairedTokens()).thenReturn(new HashSet<>());
    }

    private VnodeOnDemandRepairJob vnodeOnDemandJob(final RepairLockType lockType)
    {
        stubOngoingJob(RepairType.VNODE);
        return new VnodeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(lockType)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withNode(myNode)
                .build();
    }

    private IncrementalOnDemandRepairJob incrementalOnDemandJob(final RepairLockType lockType)
    {
        stubOngoingJob(RepairType.INCREMENTAL);
        return new IncrementalOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(lockType)
                .withReplicationState(myReplicationState)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withNode(myNode)
                .build();
    }

    @Test
    public void testScheduledVnodeJobFollowsLockType()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(vnodeJob(lockType, RepairType.VNODE).iterator()))
                    .as("scheduled VNODE job, lock type %s", lockType)
                    .containsExactlyInAnyOrderElementsOf(expectedVnodeLocks(lockType));
        }
    }

    @Test
    public void testScheduledParallelVnodeJobFollowsLockType()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(vnodeJob(lockType, RepairType.PARALLEL_VNODE).iterator()))
                    .as("scheduled PARALLEL_VNODE job, lock type %s", lockType)
                    .containsExactlyInAnyOrderElementsOf(expectedVnodeLocks(lockType));
        }
    }

    @Test
    public void testOnDemandVnodeJobFollowsLockType()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(vnodeOnDemandJob(lockType).iterator()))
                    .as("on-demand VNODE job, lock type %s", lockType)
                    .containsExactlyInAnyOrderElementsOf(expectedVnodeLocks(lockType));
        }
    }

    @Test
    public void testScheduledIncrementalJobIgnoresLockType()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(incrementalJob(lockType).iterator()))
                    .as("scheduled INCREMENTAL job, lock type %s", lockType)
                    .containsExactlyInAnyOrderElementsOf(expectedIncrementalLocks());
        }
    }

    @Test
    public void testOnDemandIncrementalJobIgnoresLockType()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(incrementalOnDemandJob(lockType).iterator()))
                    .as("on-demand INCREMENTAL job, lock type %s", lockType)
                    .containsExactlyInAnyOrderElementsOf(expectedIncrementalLocks());
        }
    }

    @Test
    public void testOnlyIncrementalUsesASingleSlotTableLock()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            assertThat(resourcesOfFirstTask(vnodeJob(lockType, RepairType.VNODE).iterator()))
                    .as("vnode job, lock type %s has no table lock", lockType)
                    .doesNotContain(INCREMENTAL_TABLE_LOCK);
            assertThat(resourcesOfFirstTask(incrementalJob(lockType).iterator()))
                    .as("incremental job, lock type %s has the table lock", lockType)
                    .contains(INCREMENTAL_TABLE_LOCK);
        }
    }
}
