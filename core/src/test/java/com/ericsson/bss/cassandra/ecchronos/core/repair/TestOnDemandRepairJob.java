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

import com.datastax.driver.core.UDTValue;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
    private ReplicationState myReplicationState;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private Node mockReplica1;

    @Mock
    private Node mockReplica2;

    @Mock
    private Node mockReplica3;

    @Mock
    private OnDemandStatus myOnDemandStatus;

    @Mock
    private OngoingJob myOngoingJob;

    @Mock
    private UDTValue myUDTValue;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testJobCorrectlyReturned()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        RepairJobView expectedView = new RepairJobView(repairJob.getId(), myTableReference, RepairConfiguration.DEFAULT, null, RepairJobView.Status.IN_QUEUE, 0);
        assertThat(repairJob.getId()).isEqualTo(repairJob.getId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getRepairConfiguration()).isEqualTo(RepairConfiguration.DEFAULT);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getRepairConfiguration()).isEqualTo(expectedView.getRepairConfiguration());
        assertThat(repairJob.getView().getRepairStateSnapshot()).isNull();
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
    }

    @Test
    public void testFailedJobCorrectlyReturned()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        RepairJobView expectedView = new RepairJobView(repairJob.getId(), myTableReference, RepairConfiguration.DEFAULT, null, RepairJobView.Status.ERROR, 0);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getRepairConfiguration()).isEqualTo(RepairConfiguration.DEFAULT);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getRepairConfiguration()).isEqualTo(expectedView.getRepairConfiguration());
        assertThat(repairJob.getView().getRepairStateSnapshot()).isNull();
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
    }

    @Test
    public void testJobFinishedAfterExecution()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
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
        OnDemandRepairJob repairJob = createRestartedOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobFailedWithWrongTokens()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        when(myReplicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(new HashMap<>());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testJobUnsuccessful()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(false, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testGetProgress()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        assertThat(repairJob.getProgress()).isEqualTo(0);
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getProgress()).isEqualTo(0.5);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    private OnDemandRepairJob createOnDemandRepairJob()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 3);
        Map<LongTokenRange, ImmutableSet<Node>> tokenRangeToReplicas = new HashMap<>();
        tokenRangeToReplicas.put(range1,
                ImmutableSet.of(mockReplica1, mockReplica2, mockReplica3));
        tokenRangeToReplicas.put(range2,
                ImmutableSet.of(mockReplica1, mockReplica2));
        when(myReplicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(tokenRangeToReplicas);

        return new OnDemandRepairJob.Builder()
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(myReplicationState)
                .withRepairHistory(myRepairHistory)
                .withOnDemandStatus(myOnDemandStatus)
                .build();
    }

    private OnDemandRepairJob createRestartedOnDemandRepairJob()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 3);
        Map<LongTokenRange, ImmutableSet<Node>> tokenRangeToReplicas = new HashMap<>();
        tokenRangeToReplicas.put(range1,
                ImmutableSet.of(mockReplica1, mockReplica2, mockReplica3));
        tokenRangeToReplicas.put(range2,
                ImmutableSet.of(mockReplica1, mockReplica2));
        when(myReplicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(tokenRangeToReplicas);

        when(myOnDemandStatus.getStartTokenFrom(any())).thenReturn(range1.start);
        when(myOnDemandStatus.getEndTokenFrom(any())).thenReturn(range1.end);

        Set<UDTValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUDTValue);

        when(myOngoingJob.getJobId()).thenReturn(UUID.randomUUID());
		when(myOngoingJob.getRepiaredTokens()).thenReturn(repiaredTokens);
        when(myOngoingJob.getTableReference()).thenReturn(myTableReference);
        when(myOngoingJob.getTokenMapHash()).thenReturn(tokenRangeToReplicas.hashCode());

        return new OnDemandRepairJob.Builder()
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(myReplicationState)
                .withRepairHistory(myRepairHistory)
                .withOnDemandStatus(myOnDemandStatus)
                .withOngoingJob(myOngoingJob)
                .build();
    }
}
