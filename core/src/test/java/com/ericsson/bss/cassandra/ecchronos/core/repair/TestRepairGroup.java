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
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
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

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private RepairStateSnapshot myRepairState;

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private RepairGroup myRepairGroup;

    @Before
    public void init()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withType(RepairOptions.RepairType.VNODE)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();

        myRepairGroup = new RepairGroup(1, tableReference,
                repairConfiguration, myRepairState, myJmxProxyFactory, myTableRepairMetrics);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myRepairState));
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testGetLockForNoDataCenters()
    {
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(), Collections.emptyList());

        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();

        try (LockFactory.DistributedLock lock = myRepairGroup.getLock(myLockFactory))
        {
            fail("Lock should not be obtained");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage(String.format("No data centers to lock for Repair job of %s.%s", keyspaceName, tableName));
        }
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

        // mock
        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();

        Collection<RepairTask> repairTasks = myRepairGroup.getRepairTasks();

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

        // mock
        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();

        Collection<RepairTask> tasks = myRepairGroup.getRepairTasks();

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

    @Test
    public void testGetLockForTwoDatacentersOneFailing() throws LockException
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC2");
        LongTokenRange range = new LongTokenRange(1, 2);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1, host2), Collections.singletonList(range));

        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());

        doReturn(new DummyLock())
                .doThrow(LockException.class)
                .when(myLockFactory).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));

        try (LockFactory.DistributedLock lock = myRepairGroup.getLock(myLockFactory))
        {
            fail("Lock should not be obtained");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage(String.format("Lock resources exhausted for Repair job of %s.%s", keyspaceName, tableName));
        }

        verify(myLockFactory).tryLock(eq("DC1"), eq("RepairResource-DC1-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).tryLock(eq("DC2"), eq("RepairResource-DC2-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-DC1-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("DC2"), eq("RepairResource-DC2-1"));
    }

    @Test
    public void testGetLockForNoLeasableDataCenters()
    {
        Host host1 = mockHost("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1), Collections.singletonList(range));

        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();

        doReturn(null).when(myLockFactory).getLockMetadata(anyString(), anyString());
        doReturn(false).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());

        try (LockFactory.DistributedLock lock = myRepairGroup.getLock(myLockFactory))
        {
            fail("Lock should not be tried to obtain");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage("Data center DC1 not lockable. Repair will be retried later.");
        }

        verify(myLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-DC1-1"));
    }

    @Test
    public void testGetLockForOneDataCenterNotLeasable() throws LockException
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC2");
        LongTokenRange range = new LongTokenRange(1, 2);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1, host2), Collections.singletonList(range));

        String resource1 = "RepairResource-DC1-1";
        String resource2 = "RepairResource-DC2-1";

        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(eq("DC2"), eq(resource2));
        doReturn(false).when(myLockFactory).sufficientNodesForLocking(eq("DC1"), eq(resource1));
        doReturn(new DummyLock()).when(myLockFactory).tryLock(eq("DC2"), eq(resource2), anyInt(), anyMapOf(String.class, String.class));

        try (LockFactory.DistributedLock lock = myRepairGroup.getLock(myLockFactory))
        {
            fail("Lock should not be obtained");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage("Data center DC1 not lockable. Repair will be retried later.");
        }

        verify(myLockFactory).sufficientNodesForLocking(eq("DC2"), eq(resource2));
        verify(myLockFactory).sufficientNodesForLocking(eq("DC1"), eq(resource1));
    }

    @Test
    public void testGetLockForTwoDatacenters() throws LockException
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC2");
        LongTokenRange range = new LongTokenRange(1, 2);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1, host2), Collections.singletonList(range));

        doReturn(replicaRepairGroup).when(myRepairState).getRepairGroup();

        doReturn(null).when(myLockFactory).getLockMetadata(anyString(), anyString());
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());
        doReturn(new DummyLock()).when(myLockFactory).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));

        myRepairGroup.getLock(myLockFactory);

        verify(myLockFactory).tryLock(eq("DC1"), eq("RepairResource-DC1-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).tryLock(eq("DC2"), eq("RepairResource-DC2-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).getLockMetadata(eq("DC1"), eq("RepairResource-DC1-1"));
        verify(myLockFactory).getLockMetadata(eq("DC2"), eq("RepairResource-DC2-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-DC1-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("DC2"), eq("RepairResource-DC2-1"));
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        return host;
    }
}
