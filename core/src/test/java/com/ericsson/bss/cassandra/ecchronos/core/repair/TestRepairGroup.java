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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairGroup
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";
    private static final TableReference tableReference = new TableReference(keyspaceName, tableName);
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

    private RepairConfiguration repairConfiguration;

    @Before
    public void init()
    {
        repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withType(RepairOptions.RepairType.VNODE)
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
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(), Collections.emptyList());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doReturn(new DummyLock()).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                myRepairResourceFactory, myRepairLockFactory);

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
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(), Collections.emptyList());
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        doReturn(repairResources).when(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        doThrow(LockException.class).when(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                myRepairResourceFactory, myRepairLockFactory);

        assertThatExceptionOfType(LockException.class).isThrownBy(() -> repairGroup.getLock(myLockFactory));

        verify(myRepairResourceFactory).getRepairResources(eq(replicaRepairGroup));
        verify(myRepairLockFactory).getLock(eq(myLockFactory), eq(repairResources), eq(metadata), eq(priority));
    }

    @Test
    public void testGetRepairTask()
    {
        // setup
        Host host = mockHost("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Host> hosts = new HashSet<>();
        hosts.add(host);

        Set<LongTokenRange> ranges = new HashSet<>();
        ranges.add(range);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(hosts, Collections.singletonList(range));

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                myRepairResourceFactory, myRepairLockFactory);

        Collection<RepairTask> repairTasks = repairGroup.getRepairTasks();

        assertThat((repairTasks).isEmpty()).isFalse();
        RepairTask repairTask = repairTasks.iterator().next();

        assertThat(repairTask.getReplicas()).isEqualTo(hosts);
        assertThat(repairTask.getTokenRanges()).isEqualTo(ranges);
        assertThat(repairTask.getTableReference()).isEqualTo(tableReference);
        assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(repairTask.getRepairConfiguration().getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(repairTask.isVnodeRepair()).isTrue();
    }

    @Test
    public void testGetPartialRepairTasks()
    {
        // setup
        Host host = mockHost("DC1");
        Host host2 = mockHost("DC1");

        List<LongTokenRange> vnodes = Arrays.asList(
                new LongTokenRange(1, 2),
                new LongTokenRange(2, 3),
                new LongTokenRange(4, 5));

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host, host2), vnodes);

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                myRepairResourceFactory, myRepairLockFactory);

        Collection<RepairTask> tasks = repairGroup.getRepairTasks();

        assertThat(tasks.size()).isEqualTo(3);

        Set<LongTokenRange> repairTaskRanges = new HashSet<>();

        for (RepairTask repairTask : tasks)
        {
            assertThat(repairTask.getTokenRanges().size()).isEqualTo(1);
            LongTokenRange range = repairTask.getTokenRanges().iterator().next();
            repairTaskRanges.add(range);

            assertThat(repairTask.getReplicas()).containsExactlyInAnyOrder(host, host2);
            assertThat(repairTask.getTableReference()).isEqualTo(tableReference);
            assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
            assertThat(repairTask.getRepairConfiguration().getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
            assertThat(repairTask.isVnodeRepair()).isTrue();
        }

        assertThat(repairTaskRanges).containsExactlyElementsOf(vnodes);
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        return host;
    }
}
