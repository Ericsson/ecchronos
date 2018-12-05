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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.MockedClock;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.DummyLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith (MockitoJUnitRunner.class)
public class TestTableRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private Metadata myMetadata;

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableMetadata myTableMetadata;

    @Mock
    private TableOptionsMetadata myTableOptionsMetadata;

    @Mock
    private RepairState myRepairState;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairFaultReporter myFaultReporter;

    private TableRepairJob myRepairJob;

    private MockedClock myClock = new MockedClock();

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);

    @Before
    public void startup()
    {
        doReturn(Sets.newHashSet()).when(myRepairState).getLocalRangesForRepair();
        doReturn(-1L).when(myRepairState).lastRepairedAt();

        doNothing().when(myRepairState).update();

        doReturn(myTableOptionsMetadata).when(myTableMetadata).getOptions();
        doReturn(myTableMetadata).when(myKeyspaceMetadata).getTable(eq(tableName));
        doReturn(myKeyspaceMetadata).when(myMetadata).getKeyspace(eq(keyspaceName));

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withType(RepairOptions.RepairType.VNODE)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();

        myRepairJob = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRepairState(myRepairState)
                .withFaultReporter(myFaultReporter)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .build();

        myRepairJob.setClock(myClock);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myMetadata));
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myRepairState));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testPrevalidateNotRepairable() throws Exception
    {
        // mock
        doReturn(false).when(myRepairState).canRepair();

        assertThat(myRepairJob.preValidate()).isFalse();

        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNeedRepair() throws Exception
    {
        // mock
        doReturn(true).when(myRepairState).canRepair();

        assertThat(myRepairJob.preValidate()).isTrue();

        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNotRepairableThenRepairable()
    {
        // mock
        doReturn(false).doReturn(true).when(myRepairState).canRepair();

        assertThat(myRepairJob.preValidate()).isFalse();
        assertThat(myRepairJob.preValidate()).isTrue();

        verify(myRepairState, times(2)).update();
        verify(myRepairState, times(2)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsOverloadException()
    {
        // mock
        doReturn(false).when(myRepairState).canRepair();
        doThrow(new OverloadedException(null, "Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.preValidate()).isFalse();

        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsException()
    {
        // mock
        doReturn(false).when(myRepairState).canRepair();
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.preValidate()).isFalse();

        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteRepaired()
    {
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteRepairedWithFailure()
    {
        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteNotRepaired()
    {
        // mock
        doReturn(true).when(myRepairState).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteNotRepairedWithFailure()
    {
        // mock
        doReturn(true).when(myRepairState).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
        verify(myRepairState, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteUpdateThrowsException()
    {
        // mock
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();
        myClock.setTime(start);

        myRepairJob.preValidate();

        // verify alarm ceased in preValidate
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
        reset(myFaultReporter);

        myRepairJob.postExecute(true);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isFalse();

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();
        myClock.setTime(start);

        myRepairJob.postExecute(true);

        // verify - repaired
        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isTrue();

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();
        myClock.setTime(start);

        myRepairJob.preValidate();

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatAlarmIsNotSentWhenGcGraceIsBelowRepairInterval()
    {
        // setup - not repaired
        int tableGcGrace = (int) TimeUnit.HOURS.toSeconds(22);
        long hoursSinceLastRepair = 23;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.HOURS.toMillis(hoursSinceLastRepair);

        // mock - not repaired
        doReturn(tableGcGrace).when(myTableOptionsMetadata).getGcGraceInSeconds();
        doReturn(lastRepaired).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();
        myClock.setTime(start);

        assertThat(myRepairJob.preValidate()).isTrue();

        // verify - not repaired
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }

    @Test
    public void testGetRepairTask()
    {
        // setup
        Host host = mock(Host.class);
        LongTokenRange range = new LongTokenRange(1, 2);

        Set<Host> hosts = new HashSet<>();
        hosts.add(host);

        Set<LongTokenRange> ranges = new HashSet<>();
        ranges.add(range);

        Map<LongTokenRange, Collection<Host>> rangeToReplicas = new HashMap<>();
        rangeToReplicas.put(new LongTokenRange(1, 2), Sets.newHashSet(host));

        // mock
        doReturn(hosts).when(myRepairState).getReplicas();
        doReturn(ranges).when(myRepairState).getLocalRangesForRepair();
        doReturn(rangeToReplicas).when(myRepairState).getRangeToReplicas();

        Iterator<ScheduledJob.ScheduledTask> iterator = myRepairJob.iterator();

        assertThat(iterator.hasNext()).isTrue();
        ScheduledJob.ScheduledTask task = iterator.next();
        assertThat(task).isInstanceOf(RepairTask.class);

        RepairTask repairTask = (RepairTask) task;

        assertThat(repairTask.getReplicas()).isEqualTo(hosts);
        assertThat(repairTask.getTokenRanges()).isEqualTo(ranges);
        assertThat(repairTask.getTableReference()).isEqualTo(myTableReference);
        assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(repairTask.getRepairConfiguration().getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(repairTask.isVnodeRepair()).isTrue();
    }

    @Test
    public void testGetPartialRepairTasks()
    {
        // setup
        Host host = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);
        Host host4 = mock(Host.class);

        Set<Host> hosts = new HashSet<>();
        hosts.add(host);
        hosts.add(host2);
        hosts.add(host3);
        hosts.add(host4);

        Set<LongTokenRange> ranges = new HashSet<>();
        ranges.add(new LongTokenRange(0, 1));
        ranges.add(new LongTokenRange(2, 3));
        ranges.add(new LongTokenRange(4, 5));
        ranges.add(new LongTokenRange(6, 7));

        Map<LongTokenRange, Collection<Host>> rangeToReplicas = new HashMap<>();
        rangeToReplicas.put(new LongTokenRange(0, 1), Sets.newHashSet(host, host2));
        rangeToReplicas.put(new LongTokenRange(2, 3), Sets.newHashSet(host2, host3));
        rangeToReplicas.put(new LongTokenRange(4, 5), Sets.newHashSet(host3, host4));

        // mock
        doReturn(hosts).when(myRepairState).getReplicas();
        doReturn(ranges).when(myRepairState).getLocalRangesForRepair();
        doReturn(rangeToReplicas).when(myRepairState).getRangeToReplicas();

        List<ScheduledJob.ScheduledTask> tasks = Lists.newArrayList(myRepairJob.iterator());

        assertThat(tasks.size()).isEqualTo(rangeToReplicas.size());

        Set<LongTokenRange> repairTaskRanges = new HashSet<>();

        for (ScheduledJob.ScheduledTask task : tasks)
        {
            assertThat(task).isInstanceOf(RepairTask.class);
            RepairTask repairTask = (RepairTask) task;

            assertThat(repairTask.getTokenRanges().size()).isEqualTo(1);
            LongTokenRange range = repairTask.getTokenRanges().iterator().next();
            repairTaskRanges.add(range);

            assertThat(repairTask.getReplicas()).isEqualTo(rangeToReplicas.get(range));
            assertThat(repairTask.getTableReference()).isEqualTo(myTableReference);
            assertThat(repairTask.getRepairConfiguration().getRepairParallelism()).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
            assertThat(repairTask.getRepairConfiguration().getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
            assertThat(repairTask.isVnodeRepair()).isTrue();
        }

        assertThat(repairTaskRanges).isEqualTo(rangeToReplicas.keySet());
    }

    @Test
    public void testGetLockForTwoDatacenters() throws LockException
    {
        doReturn(Sets.newHashSet("dc1", "dc2")).when(myRepairState).getDatacentersForRepair();

        doReturn(null).when(myLockFactory).getLockMetadata(anyString(), anyString());
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());
        doReturn(new DummyLock()).when(myLockFactory).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));

        myRepairJob.getLock(myLockFactory);

        verify(myLockFactory).tryLock(eq("dc1"), eq("RepairResource-dc1-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).tryLock(eq("dc2"), eq("RepairResource-dc2-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).getLockMetadata(eq("dc1"), eq("RepairResource-dc1-1"));
        verify(myLockFactory).getLockMetadata(eq("dc2"), eq("RepairResource-dc2-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("dc1"), eq("RepairResource-dc1-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("dc2"), eq("RepairResource-dc2-1"));
    }

    @Test
    public void testGetLockForTwoDatacentersSecondFailing() throws LockException
    {
        doReturn(Sets.newHashSet("dc1", "dc2")).when(myRepairState).getDatacentersForRepair();
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());

        doReturn(new DummyLock())
                .doThrow(LockException.class)
                .when(myLockFactory).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));

        try (LockFactory.DistributedLock lock = myRepairJob.getLock(myLockFactory))
        {
            fail("Lock should not be obtained");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage(String.format("Lock resources exhausted for Repair job of %s.%s", keyspaceName, tableName));
        }

        verify(myLockFactory).tryLock(eq("dc1"), eq("RepairResource-dc1-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).tryLock(eq("dc2"), eq("RepairResource-dc2-1"), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).sufficientNodesForLocking(eq("dc1"), eq("RepairResource-dc1-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("dc2"), eq("RepairResource-dc2-1"));
    }

    @Test
    public void testGetLockForNoDataCenters() throws LockException
    {
        doReturn(Collections.emptySet()).when(myRepairState).getDatacentersForRepair();

        try (LockFactory.DistributedLock lock = myRepairJob.getLock(myLockFactory))
        {
            fail("Lock should not be obtained");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage(String.format("No data centers to lock for Repair job of %s.%s", keyspaceName, tableName));
        }
    }

    @Test
    public void testGetLockForOneDataCenterNotLeasable() throws LockException
    {
        String dc1 = "dc1";
        String dc2 = "dc2";
        String resource1 = "RepairResource-dc1-1";
        String resource2 = "RepairResource-dc2-1";

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        doReturn(Sets.newHashSet("dc2", "dc1")).when(myRepairState).getDatacentersForRepair();
        doReturn(true).when(myLockFactory).sufficientNodesForLocking(dc2, resource2);
        doReturn(false).when(myLockFactory).sufficientNodesForLocking(dc1, resource1);
        doReturn(new DummyLock()).when(myLockFactory).tryLock(eq(dc2), eq(resource2), anyInt(), anyMapOf(String.class, String.class));

        try (LockFactory.DistributedLock lock = myRepairJob.getLock(myLockFactory))
        {
        }

        verify(myLockFactory).sufficientNodesForLocking(eq(dc2), eq(resource2));
        verify(myLockFactory).sufficientNodesForLocking(eq(dc1), eq(resource1));
        verify(myLockFactory).tryLock(eq(dc2), eq(resource2), anyInt(), anyMapOf(String.class, String.class));
        verify(myLockFactory).getLockMetadata(eq(dc2), eq(resource2));
        verify(myFaultReporter).raise(eq(FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testGetLockForNoLeasableDataCenters() throws LockException
    {
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        doReturn(Sets.newHashSet("dc1", "dc2")).when(myRepairState).getDatacentersForRepair();
        doReturn(null).when(myLockFactory).getLockMetadata(anyString(), anyString());
        doReturn(false).when(myLockFactory).sufficientNodesForLocking(anyString(), anyString());

        try (LockFactory.DistributedLock lock = myRepairJob.getLock(myLockFactory))
        {
            fail("Lock should not be tried to obtain");
        }
        catch (LockException e)
        {
            assertThat(e).hasMessage(String.format("No data centers to lock for Repair job of %s.%s", keyspaceName, tableName));
        }

        verify(myLockFactory).sufficientNodesForLocking(eq("dc1"), eq("RepairResource-dc1-1"));
        verify(myLockFactory).sufficientNodesForLocking(eq("dc2"), eq("RepairResource-dc2-1"));
        verify(myFaultReporter, times(2)).raise(eq(FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testLastSuccessfulRunIsBasedOnRepairHistory()
    {
        long timeOffset = TimeUnit.MINUTES.toMillis(1);
        long now = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2);
        long lastRepairedAtWarning = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS * 2);
        long lastRepairedAtAfterRepair = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + timeOffset;

        myClock.setTime(now);

        // We have waited 2 days to repair, send alarm and run repair
        doReturn(lastRepairedAtWarning).when(myRepairState).lastRepairedAt();
        doReturn(true).when(myRepairState).canRepair();

        assertThat(myRepairJob.runnable()).isTrue();

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtWarning);
        verify(myFaultReporter).raise(eq(FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));
        verifyNoMoreInteractions(myFaultReporter);
        reset(myFaultReporter);

        // Repair has been completed
        doReturn(lastRepairedAtAfterRepair).when(myRepairState).lastRepairedAt();
        doReturn(false).when(myRepairState).canRepair();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtAfterRepair);
        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));

        // After 10 ms we can repair again
        myClock.setTime(now + timeOffset);
        doReturn(true).when(myRepairState).canRepair();

        assertThat(myRepairJob.runnable()).isTrue();

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRepairedAtAfterRepair);
        verify(myFaultReporter, times(0)).raise(any(FaultCode.class), anyMapOf(String.class, Object.class));
    }
}
