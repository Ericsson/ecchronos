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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.IncrementalOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Iterator;
import java.util.UUID;
import java.util.function.Consumer;
import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestIncrementalOnDemandRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private OngoingJob myOngoingJob;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private Consumer<UUID> myHook;

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
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.INCREMENTAL);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testCurrentJobCorrectlyReturned()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.IN_QUEUE, 0, System.currentTimeMillis(), RepairType.INCREMENTAL);
        assertThat(repairJob.getJobId()).isEqualTo(repairJob.getJobId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
        assertThat(repairJob.getView().getRepairType()).isEqualTo(expectedView.getRepairType());
    }

    @Test
    public void testCurrentFailedJobCorrectlyReturned()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.ERROR, 0, System.currentTimeMillis(), RepairType.INCREMENTAL);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
        assertThat(repairJob.getView().getRepairType()).isEqualTo(expectedView.getRepairType());
    }

    @Test
    public void testFailedJobCorrectlyReturned()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.failed);
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.ERROR, 0, System.currentTimeMillis(), RepairType.INCREMENTAL);
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
        assertThat(repairJob.getView().getRepairType()).isEqualTo(expectedView.getRepairType());
    }

    @Test
    public void testFinishedJobCorrectlyReturned()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.finished);
        OnDemandRepairJobView expectedView = new OnDemandRepairJobView(repairJob.getJobId(), myHostId, myTableReference,
                OnDemandRepairJobView.Status.COMPLETED, 0, System.currentTimeMillis(), RepairType.INCREMENTAL);
        assertThat(repairJob.getJobId()).isEqualTo(repairJob.getJobId());
        assertThat(repairJob.getLastSuccessfulRun()).isEqualTo(-1);
        assertThat(repairJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairJob.getView().getTableReference()).isEqualTo(expectedView.getTableReference());
        assertThat(repairJob.getView().getStatus()).isEqualTo(expectedView.getStatus());
        assertThat(repairJob.getView().getNodeId()).isEqualTo(expectedView.getNodeId());
        assertThat(repairJob.getView().getRepairType()).isEqualTo(expectedView.getRepairType());
    }

    @Test
    public void testJobFinishedAfterExecution()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobFinishedAfterRestart()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FINISHED);
    }

    @Test
    public void testJobUnsuccessful()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        repairJob.postExecute(false, it.next());
        assertThat(repairJob.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testGetProgress()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        assertThat(repairJob.getProgress()).isEqualTo(0);
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(true, it.next());
        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testGetProgressWhenJobFinished()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.finished);
        assertThat(repairJob.getProgress()).isEqualTo(1);
    }

    @Test
    public void testGetProgressWhenJobFailed()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        when(myOngoingJob.getStatus()).thenReturn(OngoingJob.Status.failed);
        assertThat(repairJob.getProgress()).isEqualTo(0);
    }

    @Test
    public void testFinishJobSuccessful()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(true, it.next());
        repairJob.finishJob();
        verify(myHook).accept(any(UUID.class));
        verify(myOngoingJob).finishJob();
    }

    @Test
    public void testFinishJobFailed()
    {
        IncrementalOnDemandRepairJob repairJob = createIncrementalOnDemandRepairJob();
        Iterator<ScheduledTask> it = repairJob.iterator();
        repairJob.postExecute(false, it.next());
        repairJob.finishJob();
        verify(myHook).accept(any(UUID.class));
        verify(myOngoingJob).failJob();
    }

    private IncrementalOnDemandRepairJob createIncrementalOnDemandRepairJob()
    {
        return new IncrementalOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(myReplicationState)
                .withOngoingJob(myOngoingJob)
                .withOnFinished(myHook)
                .withNode(myNode)
                .build();
    }
}

