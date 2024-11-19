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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;


import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResourceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairGroup
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";
    private static final TableReference TABLE_REFERENCE = tableReference(KEYSPACE_NAME, TABLE_NAME);
    private static final int PRIORITY = 1;

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS_IN_DAYS = 10;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistoryService myRepairHistoryService;

    @Mock
    private RepairHistory.RepairSession myRepairSession;

    @Mock
    private Node mockNode;

    @Mock
    private RepairResourceFactory myRepairResourceFactory;

    @Mock
    private RepairLockFactory myRepairLockFactory;

    @Mock
    private LockFactory myLockFactory;

    private final UUID myNodeID = UUID.randomUUID();

    private final UUID myJobId = UUID.randomUUID();

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void init()
    {
        when(mockNode.getHostId()).thenReturn(myNodeID);
        when(myRepairHistoryService.newSession(any(), any(), any(), any(), any())).thenReturn(myRepairSession);
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS_IN_DAYS, TimeUnit.DAYS)
                .build();
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testGetLock() throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", KEYSPACE_NAME);
        metadata.put("table", TABLE_NAME);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of(), System.currentTimeMillis());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doReturn(new DummyLock()).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(PRIORITY), eq(myNodeID));

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(PRIORITY);

        repairGroup.getLock(myLockFactory, myNodeID);

        verify(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        verify(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(PRIORITY), eq(myNodeID));
    }

    @Test
    public void testGetLockWithThrowingLockingStrategy() throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", KEYSPACE_NAME);
        metadata.put("table", TABLE_NAME);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of(), System.currentTimeMillis());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doThrow(LockException.class).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(PRIORITY), eq(myNodeID));

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(PRIORITY);

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> repairGroup.getLock(myLockFactory, myNodeID));

        verify(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        verify(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(PRIORITY), eq(myNodeID));
    }

    @Test
    public void testGetIncrementalRepairTask()
    {
        DriverNode node = mockNode("DC1");
        when(node.getId()).thenReturn(myNodeID);
        ImmutableSet<DriverNode> nodes = ImmutableSet.of(node);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(), System.currentTimeMillis());
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS_IN_DAYS, TimeUnit.DAYS)
                .withRepairType(RepairType.INCREMENTAL)
                .build();

        RepairGroup repairGroup = builderFor(replicaRepairGroup).withRepairConfiguration(repairConfiguration).build(
                PRIORITY);

        Collection<RepairTask> repairTasks = repairGroup.getRepairTasks(myNodeID);

        assertThat(repairTasks).hasSize(1);
        IncrementalRepairTask repairTask = (IncrementalRepairTask) repairTasks.iterator().next();

        assertThat(repairTask.getTableReference()).isEqualTo(TABLE_REFERENCE);
        assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairParallelism.PARALLEL);
        assertThat(repairTask.getRepairConfiguration().getRepairType()).isEqualTo(RepairType.INCREMENTAL);
    }

    @Test
    public void testExecuteAllTasksSuccessful() throws ScheduledJobException
    {
        DriverNode node = mockNode("DC1");
        when(node.getId()).thenReturn(myNodeID);
        LongTokenRange range = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> nodes = ImmutableSet.of(node);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(range), System.currentTimeMillis());

        RepairGroup repairGroup = spy(builderFor(replicaRepairGroup).withNode(mockNode).build(PRIORITY));
        RepairTask repairTask1 = mock(RepairTask.class);
        RepairTask repairTask2 = mock(RepairTask.class);
        RepairTask repairTask3 = mock(RepairTask.class);
        Collection<RepairTask> tasks = new ArrayList<>();
        tasks.add(repairTask1);
        tasks.add(repairTask2);
        tasks.add(repairTask3);
        doReturn(tasks).when(repairGroup).getRepairTasks(myNodeID);
        doNothing().when(repairTask1).execute();
        doNothing().when(repairTask2).execute();
        doNothing().when(repairTask3).execute();

        boolean success = repairGroup.execute(myNodeID);
        assertThat(success).isTrue();
    }

    @Test
    public void testExecuteAllTasksFailed() throws ScheduledJobException
    {
        DriverNode node = mockNode("DC1");
        when(node.getId()).thenReturn(myNodeID);
        LongTokenRange range = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> nodes = ImmutableSet.of(node);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(range), System.currentTimeMillis());

        RepairGroup repairGroup = spy(builderFor(replicaRepairGroup).withNode(mockNode).build(PRIORITY));
        RepairTask repairTask1 = mock(RepairTask.class);
        RepairTask repairTask2 = mock(RepairTask.class);
        RepairTask repairTask3 = mock(RepairTask.class);
        Collection<RepairTask> tasks = new ArrayList<>();
        tasks.add(repairTask1);
        tasks.add(repairTask2);
        tasks.add(repairTask3);
        doReturn(tasks).when(repairGroup).getRepairTasks(myNodeID);
        doThrow(new ScheduledJobException("foo")).when(repairTask1).execute();
        doThrow(new ScheduledJobException("foo")).when(repairTask2).execute();
        doThrow(new ScheduledJobException("foo")).when(repairTask3).execute();

        boolean success = repairGroup.execute(myNodeID);
        assertThat(success).isFalse();
    }

    @Test
    public void testExecuteSomeTasksFailed() throws ScheduledJobException
    {
        DriverNode node = mockNode("DC1");
        when(node.getId()).thenReturn(myNodeID);
        LongTokenRange range = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> nodes = ImmutableSet.of(node);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(range), System.currentTimeMillis());

        RepairGroup repairGroup = spy(builderFor(replicaRepairGroup).withNode(mockNode).build(PRIORITY));
        RepairTask repairTask1 = mock(RepairTask.class);
        RepairTask repairTask2 = mock(RepairTask.class);
        RepairTask repairTask3 = mock(RepairTask.class);
        Collection<RepairTask> tasks = new ArrayList<>();
        tasks.add(repairTask1);
        tasks.add(repairTask2);
        tasks.add(repairTask3);
        doReturn(tasks).when(repairGroup).getRepairTasks(myNodeID);
        doThrow(new ScheduledJobException("foo")).when(repairTask1).execute();
        doNothing().when(repairTask2).execute();
        doThrow(new ScheduledJobException("foo")).when(repairTask3).execute();

        boolean success = repairGroup.execute(myNodeID);
        assertThat(success).isFalse();
    }

    private RepairGroup.Builder builderFor(ReplicaRepairGroup replicaRepairGroup)
    {
        return RepairGroup.newBuilder()
                .withTableReference(TABLE_REFERENCE)
                .withRepairConfiguration(myRepairConfiguration)
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairHistory(myRepairHistoryService)
                .withJobId(myJobId)
                .withRepairResourceFactory(myRepairResourceFactory)
                .withRepairLockFactory(myRepairLockFactory)
                .withNode(mockNode);
    }

    private DriverNode mockNode(String dataCenter)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
