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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange.PrimaryRangeRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange.PrimaryRangeRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairJobFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduledRepairJobFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResourceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Runs every {@link RepairType} through every place that selects behavior by repair type, and checks the outcome
 * against an explicit per-type expectation. A new {@link RepairType} value has no expectation here and fails every
 * test in this class with a message naming the dispatch point, so it cannot be silently handled as another type.
 * <p>
 * The dispatch points are written as switch expressions without a {@code default}, so a new value also fails to
 * compile there. This test is the second line of defence, and covers the case where a dispatch point is later
 * rewritten into a form that does not have that guarantee. See issue #1989.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairTypeDispatch
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table");
    private static final LongTokenRange RANGE_A = new LongTokenRange(0, 1);
    private static final LongTokenRange RANGE_B = new LongTokenRange(3, 4);
    private static final LongTokenRange RANGE_C = new LongTokenRange(6, 8);

    // Expectations. Every RepairType must have an entry in every map.
    private static final Map<RepairType, Class<?>> SCHEDULED_JOB = Map.of(
            RepairType.VNODE, TableRepairJob.class,
            RepairType.PARALLEL_VNODE, TableRepairJob.class,
            RepairType.INCREMENTAL, IncrementalRepairJob.class,
            RepairType.PRIMARY_RANGE, PrimaryRangeRepairJob.class);

    private static final Map<RepairType, Class<?>> ON_DEMAND_JOB = Map.of(
            RepairType.VNODE, VnodeOnDemandRepairJob.class,
            RepairType.PARALLEL_VNODE, VnodeOnDemandRepairJob.class,
            RepairType.INCREMENTAL, IncrementalOnDemandRepairJob.class,
            RepairType.PRIMARY_RANGE, PrimaryRangeOnDemandRepairJob.class);

    private static final Map<RepairType, List<Class<?>>> REPAIR_TASKS_FOR_THREE_RANGES = Map.of(
            RepairType.VNODE, List.of(VnodeRepairTask.class, VnodeRepairTask.class, VnodeRepairTask.class),
            RepairType.PARALLEL_VNODE, List.of(VnodeRepairTask.class),
            RepairType.INCREMENTAL, List.of(IncrementalRepairTask.class),
            RepairType.PRIMARY_RANGE, List.of(PrimaryRangeRepairTask.class));

    private static final Map<RepairType, Boolean> REQUIRES_TOKENS_PER_REPAIR = Map.of(
            RepairType.VNODE, true,
            RepairType.PARALLEL_VNODE, false,
            RepairType.INCREMENTAL, false,
            RepairType.PRIMARY_RANGE, false);

    /**
     * The token map hash persisted for the rows a cluster-wide job writes for other nodes: 0 for incremental, the
     * hash of all ranges for vnode types. Absent means no rows are written: every managed node already has its own
     * job and repairs its own primary ranges, so primary range repair must not hand out rows.
     */
    private static final Map<RepairType, Optional<Integer>> STORED_TOKEN_MAP_HASH = Map.of(
            RepairType.VNODE, Optional.of(Set.of(RANGE_A).hashCode()),
            RepairType.PARALLEL_VNODE, Optional.of(Set.of(RANGE_A).hashCode()),
            RepairType.INCREMENTAL, Optional.of(0),
            RepairType.PRIMARY_RANGE, Optional.empty());

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private RepairFaultReporter myFaultReporter;
    @Mock
    private RepairStateFactory myRepairStateFactory;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private CassandraMetrics myCassandraMetrics;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private RepairHistoryService myRepairHistoryService;
    @Mock
    private RepairHistory.RepairSession myRepairSession;
    @Mock
    private TableStorageStates myTableStorageStates;
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;
    @Mock
    private RepairState myRepairState;
    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;
    @Mock
    private RepairResourceFactory myRepairResourceFactory;
    @Mock
    private RepairLockFactory myRepairLockFactory;
    @Mock
    private OnDemandStatus myOnDemandStatus;
    @Mock
    private OngoingJob myOngoingJob;
    @Mock
    private BiConsumer<UUID, UUID> myOnFinishedHook;
    @Mock
    private Node myNode;

    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myReplicaId = UUID.randomUUID();
    private DriverNode myReplica;

    @Before
    public void setup()
    {
        myReplica = mock(DriverNode.class);
        when(myReplica.getId()).thenReturn(myReplicaId);
        when(myReplica.getDatacenter()).thenReturn("DC1");

        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairHistoryService.newSession(any(), any(), any(), any(), any(), any())).thenReturn(myRepairSession);
        when(myReplicationState.getReplicas(TABLE_REFERENCE, myNode)).thenReturn(ImmutableSet.of(myReplica));

        VnodeRepairState vnodeRepairState =
                TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());
        when(myRepairState.getSnapshot()).thenReturn(myRepairStateSnapshot);
        when(myRepairStateFactory.create(any(), any(), any(), any())).thenReturn(myRepairState);
        when(myRepairStateSnapshot.getVnodeRepairStates())
                .thenReturn(VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState)).build());
    }

    private static <T> T expectationFor(final Map<RepairType, T> expectations, final RepairType type,
            final String dispatchPoint)
    {
        T expectation = expectations.get(type);
        assertThat(expectation)
                .as("%s has no expectation for repair type %s. Add one here, and make sure the dispatch point "
                        + "handles the type explicitly instead of falling through to another type",
                        dispatchPoint, type)
                .isNotNull();
        return expectation;
    }

    private static RepairConfiguration configurationFor(final RepairType type)
    {
        return RepairConfiguration.newBuilder().withRepairType(type).build();
    }

    @Test
    public void testScheduledRepairJobFactoryHandlesEveryRepairType()
    {
        ScheduledRepairJobFactory factory = ScheduledRepairJobFactory.builder()
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairHistoryService(myRepairHistoryService)
                .withFaultReporter(myFaultReporter)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRepairStateFactory(myRepairStateFactory)
                .withReplicationState(myReplicationState)
                .withCassandraMetrics(myCassandraMetrics)
                .withRepairPolicies(new ArrayList<>())
                .withTableStorageStates(myTableStorageStates)
                .withRepairLockType(RepairLockType.VNODE)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build();

        for (RepairType type : RepairType.values())
        {
            Class<?> expected = expectationFor(SCHEDULED_JOB, type, "ScheduledRepairJobFactory.create");

            ScheduledRepairJob job = factory.create(myNode, TABLE_REFERENCE, configurationFor(type));

            assertThat(job).as("scheduled job for %s", type).isExactlyInstanceOf(expected);
            assertThat(job.getRepairConfiguration().getRepairType()).isEqualTo(type);
        }
    }

    @Test
    public void testOnDemandRepairJobFactoryHandlesEveryRepairType()
    {
        Map<UUID, Node> nodes = Map.of(myNodeId, myNode);
        when(myOnDemandStatus.getNodes()).thenReturn(nodes);
        when(myOngoingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngoingJob.getHostId()).thenReturn(myNodeId);
        when(myOngoingJob.getJobId()).thenReturn(UUID.randomUUID());

        OnDemandRepairJobFactory factory = OnDemandRepairJobFactory.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withReplicationState(myReplicationState)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistoryService)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withOnDemandStatus(myOnDemandStatus)
                .withOnFinishedHook(myOnFinishedHook)
                .build();

        for (RepairType type : RepairType.values())
        {
            Class<?> expected = expectationFor(ON_DEMAND_JOB, type, "OnDemandRepairJobFactory.createFromOngoingJob");
            when(myOngoingJob.getRepairType()).thenReturn(type);

            OnDemandRepairJob job = factory.createFromOngoingJob(myOngoingJob);

            assertThat(job).as("on-demand job for %s", type).isExactlyInstanceOf(expected);
            assertThat(job.getRepairConfiguration().getRepairType()).isEqualTo(type);
        }
    }

    @Test
    public void testRepairGroupCreatesTasksForEveryRepairType()
    {
        for (RepairType type : RepairType.values())
        {
            List<Class<?>> expected = expectationFor(REPAIR_TASKS_FOR_THREE_RANGES, type,
                    "RepairGroup.getRepairTasks");

            Collection<RepairTask> tasks = repairGroupBuilder(type).build(1).getRepairTasks(myNodeId);

            assertThat(tasks.stream().<Class<?>>map(Object::getClass).toList())
                    .as("repair tasks for %s", type)
                    .containsExactlyElementsOf(expected);
        }
    }

    @Test
    public void testRepairGroupRequiresTokensPerRepairForEveryRepairTypeThatSplitsRanges()
    {
        for (RepairType type : RepairType.values())
        {
            boolean required = expectationFor(REQUIRES_TOKENS_PER_REPAIR, type, "RepairGroup constructor");
            RepairGroup.Builder builder = repairGroupBuilder(type).withTokensPerRepair(null);

            if (required)
            {
                assertThatExceptionOfType(NullPointerException.class)
                        .as("%s requires tokens per repair", type)
                        .isThrownBy(() -> builder.build(1))
                        .withMessageContaining("Tokens per repair must be set");
            }
            else
            {
                assertThat(builder.build(1)).as("%s does not require tokens per repair", type).isNotNull();
            }
        }
    }

    @Test
    public void testClusterWideOnDemandJobPersistsStateForEveryRepairType()
    {
        when(myOnDemandStatus.getNodes()).thenReturn(Map.of(myNodeId, myNode));
        when(myReplicationState.getTokenRangeToReplicas(TABLE_REFERENCE, myNode)).thenReturn(Collections.emptyMap());
        when(myReplicationState.getTokenRanges(TABLE_REFERENCE, myNode))
                .thenReturn(Map.of(RANGE_A, ImmutableSet.of(myReplica)));

        for (RepairType type : RepairType.values())
        {
            Optional<Integer> expectedHash = expectationFor(STORED_TOKEN_MAP_HASH, type,
                    "OngoingJob.startClusterWideJob");
            clearInvocations(myOnDemandStatus);
            OngoingJob job = new OngoingJob.Builder()
                    .withOnDemandStatus(myOnDemandStatus)
                    .withTableReference(TABLE_REFERENCE)
                    .withReplicationState(myReplicationState)
                    .withHostId(myNodeId)
                    .withRepairType(type)
                    .build();

            // The constructor writes the job's own row. Only what startClusterWideJob adds is checked below.
            clearInvocations(myOnDemandStatus);
            job.startClusterWideJob(type);

            if (expectedHash.isEmpty())
            {
                verify(myOnDemandStatus, never()).addNewJob(any(), any(), any(), anyInt(), any(), any());
                continue;
            }
            ArgumentCaptor<Integer> hash = ArgumentCaptor.forClass(Integer.class);
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Set<LongTokenRange>> repaired = ArgumentCaptor.forClass(Set.class);
            verify(myOnDemandStatus).addNewJob(eq(myReplicaId), eq(job.getJobId()), eq(TABLE_REFERENCE),
                    hash.capture(), repaired.capture(), eq(type));
            assertThat(hash.getValue()).as("stored token map hash for %s", type).isEqualTo(expectedHash.get());
            assertThat(repaired.getValue() == null || repaired.getValue().isEmpty())
                    .as("no ranges are marked repaired when %s is created", type).isTrue();
        }
    }

    private RepairGroup.Builder repairGroupBuilder(final RepairType type)
    {
        DriverNode first = mock(DriverNode.class);
        when(first.getId()).thenReturn(myReplicaId);
        when(first.getDatacenter()).thenReturn("DC1");
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(first),
                ImmutableList.of(RANGE_A, RANGE_B, RANGE_C), System.currentTimeMillis());
        return RepairGroup.newBuilder()
                .withTableReference(TABLE_REFERENCE)
                .withRepairConfiguration(configurationFor(type))
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairHistory(myRepairHistoryService)
                .withJobId(UUID.randomUUID())
                .withRepairResourceFactory(myRepairResourceFactory)
                .withRepairLockFactory(myRepairLockFactory)
                .withNode(myNode);
    }
}
