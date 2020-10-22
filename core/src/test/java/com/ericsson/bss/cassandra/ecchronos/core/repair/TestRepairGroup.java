/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairGroup
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";
    private static final TableReference tableReference = tableReference(keyspaceName, tableName);
    private static final int priority = 1;

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairResourceFactory myRepairResourceFactory;

    @Mock
    private RepairLockFactory myRepairLockFactory;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private RepairHistory.RepairSession myRepairSession;

    private final UUID myJobId = UUID.randomUUID();

    private RepairConfiguration repairConfiguration;

    @Before
    public void init()
    {
        when(myRepairHistory.newSession(any(), any(), any(), any())).thenReturn(myRepairSession);

        repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testGetLock() throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", keyspaceName);
        metadata.put("table", tableName);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doReturn(new DummyLock()).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(priority);

        repairGroup.getLock(myLockFactory);

        verify(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        verify(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));
    }

    @Test
    public void testGetLockWithThrowingLockingStrategy() throws LockException
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", keyspaceName);
        metadata.put("table", tableName);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(), ImmutableList.of());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doThrow(LockException.class).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(priority);

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> repairGroup.getLock(myLockFactory));

        verify(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        verify(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));
    }

    @Test
    public void testGetRepairTask()
    {
        // setup
        Node node = mockNode("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);

        ImmutableSet<Node> nodes = ImmutableSet.of(node);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(range));

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(priority);

        Collection<RepairTask> repairTasks = repairGroup.getRepairTasks();

        assertThat(repairTasks).hasSize(1);
        RepairTask repairTask = repairTasks.iterator().next();

        assertThat(repairTask.getReplicas()).containsExactlyInAnyOrderElementsOf(nodes);
        assertThat(repairTask.getTokenRanges()).containsExactly(range);
        assertThat(repairTask.getTableReference()).isEqualTo(tableReference);
        assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
    }

    @Test
    public void testGetRepairTaskWithSubRange()
    {
        List<LongTokenRange> expectedTokenRanges = Arrays.asList(
                new LongTokenRange(0, 1),
                new LongTokenRange(1, 2),
                new LongTokenRange(2, 3),
                new LongTokenRange(3, 4),
                new LongTokenRange(4, 5)
        );

        BigInteger tokensPerRange = BigInteger.ONE;

        // setup
        Node node = mockNode("DC1");
        LongTokenRange vnode = new LongTokenRange(0, 5);

        ImmutableSet<Node> nodes = ImmutableSet.of(node);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(nodes, ImmutableList.of(vnode));

        RepairGroup repairGroup = builderFor(replicaRepairGroup)
                .withTokensPerRepair(tokensPerRange)
                .build(priority);

        Collection<RepairTask> repairTasks = repairGroup.getRepairTasks();

        assertThat(repairTasks).hasSize(5);
        Iterator<RepairTask> iterator = repairTasks.iterator();

        for (LongTokenRange expectedRange : expectedTokenRanges)
        {
            assertThat(iterator.hasNext()).isTrue();
            RepairTask repairTask = iterator.next();

            assertThat(repairTask.getReplicas()).containsExactlyInAnyOrderElementsOf(nodes);
            assertThat(repairTask.getTokenRanges()).containsExactly(expectedRange);
            assertThat(repairTask.getTableReference()).isEqualTo(tableReference);
            assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        }
    }

    @Test
    public void testGetPartialRepairTasks()
    {
        // setup
        Node node = mockNode("DC1");
        Node node2 = mockNode("DC1");

        ImmutableList<LongTokenRange> vnodes = ImmutableList.of(
                new LongTokenRange(1, 2),
                new LongTokenRange(2, 3),
                new LongTokenRange(4, 5));

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(node, node2), vnodes);

        RepairGroup repairGroup = builderFor(replicaRepairGroup).build(priority);

        Collection<RepairTask> tasks = repairGroup.getRepairTasks();

        assertThat(tasks.size()).isEqualTo(3);

        Set<LongTokenRange> repairTaskRanges = new HashSet<>();

        for (RepairTask repairTask : tasks)
        {
            assertThat(repairTask.getTokenRanges().size()).isEqualTo(1);
            LongTokenRange range = repairTask.getTokenRanges().iterator().next();
            repairTaskRanges.add(range);

            assertThat(repairTask.getReplicas()).containsExactlyInAnyOrder(node, node2);
            assertThat(repairTask.getTableReference()).isEqualTo(tableReference);
            assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        }

        assertThat(repairTaskRanges).containsExactlyElementsOf(vnodes);
    }

    private RepairGroup.Builder builderFor(ReplicaRepairGroup replicaRepairGroup)
    {
        return RepairGroup.newBuilder()
                .withTableReference(tableReference)
                .withRepairConfiguration(repairConfiguration)
                .withReplicaRepairGroup(replicaRepairGroup)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairResourceFactory(myRepairResourceFactory)
                .withRepairLockFactory(myRepairLockFactory)
                .withRepairHistory(myRepairHistory)
                .withJobId(myJobId);
    }

    private Node mockNode(String dataCenter)
    {
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
