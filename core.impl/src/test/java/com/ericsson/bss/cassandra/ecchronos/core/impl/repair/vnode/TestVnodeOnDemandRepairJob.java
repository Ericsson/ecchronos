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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;


import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.VnodeOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeOnDemandRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private DriverNode mockReplica1;

    @Mock
    private DriverNode mockReplica2;

    @Mock
    private DriverNode mockReplica3;

    @Mock
    private OngoingJob myOngoingJob;

    @Mock
    private Node myNode;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);
    private final UUID myHostId = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(myOngoingJob.getTableReference()).thenReturn(myTableReference);
        UUID uuid = UUID.randomUUID();
        when(myOngoingJob.getJobId()).thenReturn(uuid);
        when(myOngoingJob.getHostId()).thenReturn(myHostId);
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
        VnodeOnDemandRepairJob repairJob = createVnodeOnDemandRepairJob(0);
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.IN_QUEUE, 0, System.currentTimeMillis(), RepairType.VNODE);
        assertThat(repairJob.getJobId()).isEqualTo(repairJob.getJobId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
    }

    @Test
    public void testFailedJobCorrectlyReturned()
    {
        VnodeOnDemandRepairJob repairJob = createVnodeOnDemandRepairJob(0);
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.ERROR, 0, System.currentTimeMillis(), RepairType.VNODE);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
    }

    @Test
    public void testJobFinishedAfterExecution()
    {
        VnodeOnDemandRepairJob repairJob = createVnodeOnDemandRepairJob(0);
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobFinishedAfterRestart()
    {
        VnodeOnDemandRepairJob repairJob = createRestartedOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobFailedWhenTopologyChange()
    {
        VnodeOnDemandRepairJob repairJob = createVnodeOnDemandRepairJob(0);
        when(myOngoingJob.hasTopologyChanged()).thenReturn(true);
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testJobUnsuccessful()
    {
        VnodeOnDemandRepairJob repairJob = createVnodeOnDemandRepairJob(0);
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(false, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testGetProgress()
    {
        VnodeOnDemandRepairJob repairJobZeroProgress = createVnodeOnDemandRepairJob(0);
        assertThat(repairJobZeroProgress.getProgress()).isEqualTo(0);

        VnodeOnDemandRepairJob repairJobHalfProgress = createVnodeOnDemandRepairJob(50);
        assertThat(repairJobHalfProgress.getProgress()).isEqualTo(0.5);

        VnodeOnDemandRepairJob repairJobFullProgress = createVnodeOnDemandRepairJob(100);
        assertThat(repairJobFullProgress.getProgress()).isEqualTo(1.0);

        when(repairJobHalfProgress.getOngoingJob().getStatus()).thenReturn(OngoingJob.Status.finished);
        assertThat(repairJobHalfProgress.getProgress()).isEqualTo(1.0);
    }

    private VnodeOnDemandRepairJob createVnodeOnDemandRepairJob(int repairedTokenPercentage)
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 3);
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicas = new HashMap<>();
        tokenRangeToReplicas.put(range1,
                ImmutableSet.of(mockReplica1, mockReplica2, mockReplica3));
        tokenRangeToReplicas.put(range2,
                ImmutableSet.of(mockReplica1, mockReplica2));

        Set<LongTokenRange> repairedTokens = new HashSet<>();
        if (repairedTokenPercentage >= 50)
        {
            repairedTokens.add(range1);
        }
        if (repairedTokenPercentage == 100)
        {
            repairedTokens.add(range2);
        }

        when(myOngoingJob.getTokens()).thenReturn(tokenRangeToReplicas);
        when(myOngoingJob.getRepairedTokens()).thenReturn(repairedTokens);

        return new VnodeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withNode(myNode)
                .build();
    }

    private VnodeOnDemandRepairJob createRestartedOnDemandRepairJob()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 3);
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicas = new HashMap<>();
        tokenRangeToReplicas.put(range1,
                ImmutableSet.of(mockReplica1, mockReplica2, mockReplica3));
        tokenRangeToReplicas.put(range2,
                ImmutableSet.of(mockReplica1, mockReplica2));
        when(myOngoingJob.getTokens()).thenReturn(tokenRangeToReplicas);

        Set<LongTokenRange> repairedTokens = new HashSet<>();
        repairedTokens.add(range1);
        when(myOngoingJob.getRepairedTokens()).thenReturn(repairedTokens);

        return new VnodeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .withNode(myNode)
                .build();
    }
}

