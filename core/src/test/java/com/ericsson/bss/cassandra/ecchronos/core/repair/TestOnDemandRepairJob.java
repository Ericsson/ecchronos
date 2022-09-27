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
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
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
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
    private DriverNode mockReplica1;

    @Mock
    private DriverNode mockReplica2;

    @Mock
    private DriverNode mockReplica3;

    @Mock
    private OngoingJob myOngoingJob;

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
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.IN_QUEUE, 0, System.currentTimeMillis());
        assertThat(repairJob.getId()).isEqualTo(repairJob.getId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getHostId()).isEqualTo(expectedView.getHostId());
    }

    @Test
    public void testFailedJobCorrectlyReturned()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.ERROR, 0, System.currentTimeMillis());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getHostId()).isEqualTo(expectedView.getHostId());
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
    public void testJobFailedWhenTopologyChange()
    {
        OnDemandRepairJob repairJob = createOnDemandRepairJob();
        when(myOngoingJob.hasTopologyChanged()).thenReturn(true);
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
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicas = new HashMap<>();
        tokenRangeToReplicas.put(range1,
                ImmutableSet.of(mockReplica1, mockReplica2, mockReplica3));
        tokenRangeToReplicas.put(range2,
                ImmutableSet.of(mockReplica1, mockReplica2));
        when(myOngoingJob.getTokens()).thenReturn(tokenRangeToReplicas);

        return new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .build();
    }

    private OnDemandRepairJob createRestartedOnDemandRepairJob()
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

        return new OnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(myOngoingJob)
                .build();
    }
}
