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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

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
    private Host mockReplica1;

    @Mock
    private Host mockReplica2;

    @Mock
    private Host mockReplica3;

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);

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
        RepairJobView expectedView = new RepairJobView(repairJob.getId(), myTableReference, RepairConfiguration.DEFAULT, null);
        assertThat(repairJob.getId()).isEqualTo(repairJob.getId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getRepairConfiguration()).isEqualTo(RepairConfiguration.DEFAULT);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getRepairConfiguration()).isEqualTo(expectedView.getRepairConfiguration());
        assertThat(repairJob.getView().getRepairStateSnapshot()).isNull();
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
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

    private OnDemandRepairJob createOnDemandRepairJob()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 3);
        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicas = new HashMap<>();
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
                .build();
    }
}
