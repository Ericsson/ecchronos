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

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private OngoingJob myOngoingJob;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private RepairState myRepairState;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);

    @Before
    public void setup()
    {
        when(myOngoingJob.getTableReference()).thenReturn(myTableReference);
        UUID uuid = UUID.randomUUID();
        when(myOngoingJob.getJobId()).thenReturn(uuid );
        when(myOngoingJob.getStartedTime()).thenReturn(System.currentTimeMillis());
        when(myOngoingJob.getCompletedTime()).thenReturn(-1L);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testJobCorrectlyReturned()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob(System.currentTimeMillis());
        RepairJobView expectedView = new OnDemandRepairJobView(repairJob.getId(), myTableReference, RepairConfiguration.DISABLED, RepairJobView.Status.IN_QUEUE, 0, System.currentTimeMillis());
        assertThat(repairJob.getId()).isEqualTo(repairJob.getId());
        //assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getRepairConfiguration()).isEqualTo(RepairConfiguration.DISABLED);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getRepairConfiguration()).isEqualTo(expectedView.getRepairConfiguration());
        assertThat(repairJob.getView().getRepairStateSnapshot()).isNull();
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
    }

    @Test
    public void testFailedJobCorrectlyReturned()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob(System.currentTimeMillis());
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        RepairJobView expectedView = new OnDemandRepairJobView(repairJob.getId(), myTableReference, RepairConfiguration.DISABLED, RepairJobView.Status.ERROR, 0, System.currentTimeMillis());
        //assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getRepairConfiguration()).isEqualTo(RepairConfiguration.DISABLED);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getRepairConfiguration()).isEqualTo(expectedView.getRepairConfiguration());
        assertThat(repairJob.getView().getRepairStateSnapshot()).isNull();
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
    }

    @Test
    public void testJobFinishedAfterRestart()
    {
        long ongoingJobStarted = System.currentTimeMillis();
        long lastRepair = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10);
        when(myOngoingJob.getStartedTime()).thenReturn(ongoingJobStarted);
        OnDemandRepairJob repairJob = createRestartedOnDemandRepairJob(lastRepair);
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        mockRepairStateSnapshot(ongoingJobStarted);
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobFailedWhenTopologyChange()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob(System.currentTimeMillis());
        when(myOngoingJob.hasTopologyChanged()).thenReturn(true);
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testJobUnsuccessful()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10));
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(false, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testGetProgress()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10));
        assertThat(repairJob.getProgress()).isEqualTo(0);
        mockRepairStateSnapshot(System.currentTimeMillis());
        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    private OnDemandRepairJob createOnDemandRepairJob(long lastRepairedAt)
    {
        mockRepairStateSnapshot(lastRepairedAt);
        return new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withTableStorageStates(myTableStorageStates)
                .withRepairState(myRepairState)
                .build();
    }

    private OnDemandRepairJob createRestartedOnDemandRepairJob(long lastRepairedAt)
    {
        mockRepairStateSnapshot(lastRepairedAt);
        return new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withTableStorageStates(myTableStorageStates)
                .withRepairState(myRepairState)
                .build();
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
