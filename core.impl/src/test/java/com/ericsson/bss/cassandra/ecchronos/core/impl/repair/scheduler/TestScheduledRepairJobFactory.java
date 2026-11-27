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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.singlesession.SingleSessionRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestScheduledRepairJobFactory
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table1");
    private static final VnodeRepairState VNODE_REPAIR_STATE = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), System.currentTimeMillis());

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
    private TableStorageStates myTableStorageStates;
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;
    @Mock
    private Node myNode;
    @Mock
    private RepairState myRepairState;
    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    private ScheduledRepairJobFactory myFactory;
    private final UUID myNodeId = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(myNode.getHostId()).thenReturn(myNodeId);
        Mockito.lenient().when(myRepairState.getSnapshot()).thenReturn(myRepairStateSnapshot);
        Mockito.lenient().when(myRepairStateFactory.create(eq(myNode), eq(TABLE_REFERENCE), any(), any())).thenReturn(myRepairState);
        VnodeRepairStatesImpl vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(VNODE_REPAIR_STATE)).build();
        Mockito.lenient().when(myRepairStateSnapshot.getVnodeRepairStates()).thenReturn(vnodeRepairStates);

        myFactory = ScheduledRepairJobFactory.builder()
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
    }

    @Test
    public void testCreateVnodeRepairJob()
    {
        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        assertThat(job).isInstanceOf(TableRepairJob.class);
        assertThat(job.getTableReference()).isEqualTo(TABLE_REFERENCE);
        assertThat(job.getRepairConfiguration()).isEqualTo(RepairConfiguration.DEFAULT);
    }

    @Test
    public void testCreateParallelVnodeRepairJob()
    {
        // PARALLEL_VNODE is not a separate job class: it is created as a TableRepairJob like VNODE. Pinned so that
        // making the factory dispatch explicit (issue #1989) does not change it.
        RepairConfiguration parallelConfig = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.PARALLEL_VNODE)
                .build();

        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, parallelConfig);

        assertThat(job).isInstanceOf(TableRepairJob.class);
        assertThat(job.getTableReference()).isEqualTo(TABLE_REFERENCE);
        assertThat(job.getRepairConfiguration()).isEqualTo(parallelConfig);
    }

    @Test
    public void testCreateSingleSessionRepairJob()
    {
        RepairConfiguration primaryRangeConfig = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.PRIMARY_RANGE)
                .build();

        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, primaryRangeConfig);

        assertThat(job).isExactlyInstanceOf(SingleSessionRepairJob.class);
        assertThat(job.getTableReference()).isEqualTo(TABLE_REFERENCE);
        assertThat(job.getRepairConfiguration()).isEqualTo(primaryRangeConfig);
        assertThat(job.getView().getRepairType()).isEqualTo(RepairType.PRIMARY_RANGE);
    }

    @Test
    public void testSingleSessionRepairJobRaisesAlarmsInTheFaultReporterOfTheFactory()
    {
        RepairConfiguration primaryRangeConfig = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.PRIMARY_RANGE)
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairWarningTime(2, TimeUnit.DAYS)
                .withRepairErrorTime(3, TimeUnit.DAYS)
                .build();
        long lastRepaired = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
        when(myRepairHistoryService.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenReturn(OptionalLong.of(lastRepaired));
        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, primaryRangeConfig);

        job.refreshState();

        Mockito.verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), any());
    }

    @Test
    public void testCreateAllReplicatedRangesRepairJob()
    {
        RepairConfiguration config = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.ALL_REPLICATED_RANGES)
                .build();

        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, config);

        assertThat(job).isExactlyInstanceOf(SingleSessionRepairJob.class);
        assertThat(job.getRepairConfiguration()).isEqualTo(config);
        assertThat(job.getView().getRepairType()).isEqualTo(RepairType.ALL_REPLICATED_RANGES);
    }

    private ScheduledRepairJobFactory factoryWithManagedNodes()
    {
        return ScheduledRepairJobFactory.builder()
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
                .withManagedNodes(() -> java.util.List.of(myNode))
                .build();
    }

    @Test
    public void testAllReplicatedRangesJobChecksWhichRangesTheManagedNodesReach()
    {
        RepairConfiguration config = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.ALL_REPLICATED_RANGES).build();

        factoryWithManagedNodes().create(myNode, TABLE_REFERENCE, config);

        Mockito.verify(myReplicationState).getTokenRanges(TABLE_REFERENCE, myNode);
    }

    @Test
    public void testJobsOfOtherRepairTypesDoNotCheckWhichRangesAreReached()
    {
        for (RepairType type : new RepairType[] {RepairType.PRIMARY_RANGE, RepairType.VNODE})
        {
            factoryWithManagedNodes().create(myNode, TABLE_REFERENCE,
                    RepairConfiguration.newBuilder().withRepairType(type).build());
        }

        Mockito.verify(myReplicationState, Mockito.never()).getTokenRanges(any(), any());
    }

    @Test
    public void testWithoutManagedNodesThereIsNoCheck()
    {
        myFactory.create(myNode, TABLE_REFERENCE,
                RepairConfiguration.newBuilder().withRepairType(RepairType.ALL_REPLICATED_RANGES).build());

        Mockito.verify(myReplicationState, Mockito.never()).getTokenRanges(any(), any());
    }

    @Test
    public void testCreateIncrementalRepairJob()
    {
        RepairConfiguration incrementalConfig = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.INCREMENTAL)
                .build();

        ScheduledRepairJob job = myFactory.create(myNode, TABLE_REFERENCE, incrementalConfig);

        assertThat(job).isInstanceOf(IncrementalRepairJob.class);
        assertThat(job.getTableReference()).isEqualTo(TABLE_REFERENCE);
        assertThat(job.getRepairConfiguration()).isEqualTo(incrementalConfig);
    }
}
