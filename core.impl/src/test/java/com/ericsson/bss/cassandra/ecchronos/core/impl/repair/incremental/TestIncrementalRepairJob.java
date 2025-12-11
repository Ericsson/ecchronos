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
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import java.util.UUID;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestIncrementalRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";
    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long WARNING_IN_DAYS = 7;
    private static final long ERROR_IN_DAYS = 10;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private CassandraMetrics myCassandraMetrics;

    @Mock
    private Node mockNode;

    @Mock
    private LockFactory myLockFactory;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);
    private RepairConfiguration myRepairConfiguration;
    private final UUID mockNodeID = UUID.randomUUID();

    @Before
    public void startup()
    {
        doReturn(0L).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        doReturn(mockNodeID).when(mockNode).getHostId();
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS).withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withRepairType(RepairType.INCREMENTAL).build();
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myReplicationState));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
    }

    @Test
    public void testGetViewNothingRepaired()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis();
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.COMPLETED);
    }

    @Test
    public void testGetViewEverythingRepaired()
    {
        doReturn(100.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis();
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(1.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.COMPLETED);
    }

    @Test
    public void testGetViewBlocked()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS);
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();
        job.setRunnableIn(TimeUnit.HOURS.toMillis(1));

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.BLOCKED);
    }

    @Test
    public void testGetViewOnTime()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS);
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.ON_TIME);
    }

    @Test
    public void testGetViewLate()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(WARNING_IN_DAYS);
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.LATE);
    }

    @Test
    public void testGetViewOverdue()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS);
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();

        assertThat(job).isNotNull();
        ScheduledRepairJobView view = job.getView();
        assertThat(view).isNotNull();
        assertThat(view.getRepairConfiguration()).isEqualTo(job.getRepairConfiguration());
        assertThat(view.getTableReference()).isEqualTo(myTableReference);
        assertThat(view.getProgress()).isEqualTo(0.0d);
        assertThat(view.getNextRepair()).isEqualTo(lastRepairedAt + TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS));
        assertThat(view.getCompletionTime()).isEqualTo(lastRepairedAt);
        assertThat(view.getStatus()).isEqualTo(ScheduledRepairJobView.Status.OVERDUE);
    }

    @Test
    public void testRunnableNothingRepaired()
    {
        doReturn(0.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();
        job.refreshState();

        assertThat(job).isNotNull();
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testRunnableHalfRepaired()
    {
        doReturn(50.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();
        job.refreshState();

        assertThat(job).isNotNull();
        assertThat(job.runnable()).isTrue();
    }

    @Test
    public void testRunnableEverythingRepaired()
    {
        doReturn(100.0d).when(myCassandraMetrics).getPercentRepaired(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();
        job.refreshState();

        assertThat(job).isNotNull();
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testRunnableIntervalNotYetPassed()
    {
        long lastRepairedAt = System.currentTimeMillis();
        doReturn(lastRepairedAt).when(myCassandraMetrics).getMaxRepairedAt(mockNodeID, myTableReference);
        IncrementalRepairJob job = getIncrementalRepairJob();
        job.refreshState();

        assertThat(job).isNotNull();
        assertThat(job.runnable()).isFalse();
    }

    @Test
    public void testIterator()
    {
        DriverNode node1 = mock(DriverNode.class);
        DriverNode node2 = mock(DriverNode.class);
        ImmutableSet<DriverNode> replicas = ImmutableSet.of(node1, node2);
        doReturn(replicas).when(myReplicationState).getReplicas(myTableReference, mockNode);
        IncrementalRepairJob job = getIncrementalRepairJob();
        when(myJmxProxyFactory.getMyHeathCheckInterval()).thenReturn(10);

        assertThat(job).isNotNull();
        Iterator<ScheduledTask> iterator = job.iterator();
        ScheduledTask task = iterator.next();
        assertThat(task).isInstanceOf(RepairGroup.class);
        Collection<RepairTask> repairTasks = ((RepairGroup) task).getRepairTasks(mockNodeID);
        assertThat(repairTasks).hasSize(1);
        IncrementalRepairTask repairTask = (IncrementalRepairTask) repairTasks.iterator().next();
        assertThat(repairTask.getRepairConfiguration()).isEqualTo(myRepairConfiguration);
        assertThat(repairTask.getTableReference()).isEqualTo(myTableReference);
        verify(myReplicationState).getReplicas(myTableReference, mockNode);
    }

    @Test
    public void testEqualsAndHashcode()
    {
        EqualsVerifier.simple().forClass(IncrementalRepairJob.class).withRedefinedSuperclass().verify();
    }

    private IncrementalRepairJob getIncrementalRepairJob()
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder().withPriority(
                ScheduledJob.Priority.LOW).withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS).build();

        return new IncrementalRepairJob.Builder().withConfiguration(configuration).withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory).withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics).withRepairConfiguration(myRepairConfiguration)
                .withCassandraMetrics(myCassandraMetrics)
                .withNode(mockNode)
                .withRepairLockType(RepairLockType.VNODE)
                .build();
    }
}
