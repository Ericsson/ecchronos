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

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils.getFailedRepairMessage;
import static com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils.getNotificationData;
import static com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils.getRepairMessage;
import static com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils.startRepair;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.Notification;
import javax.management.remote.JMXConnectionNotification;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairTask.ProgressEventType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestVnodeRepairTask
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistory repairHistory;

    private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;

    private UUID jobId = UUID.randomUUID();

    private TestUtils.MockedJmxProxy proxy = new TestUtils.MockedJmxProxy(KEYSPACE_NAME, TABLE_NAME);

    private final TableReference myTableReference = tableReference(KEYSPACE_NAME, TABLE_NAME);

    private Set<DriverNode> participants = Sets.newHashSet(mockNode(), mockNode());

    private ConcurrentMap<LongTokenRange, RepairHistory.RepairSession> repairSessions = new ConcurrentHashMap<>();

    @Before
    public void setup() throws IOException
    {
        when(jmxProxyFactory.connect()).thenReturn(proxy);
        when(repairHistory.newSession(eq(myTableReference), eq(jobId), any(), eq(participants)))
                .thenAnswer(invocation ->
                {
                    LongTokenRange range = invocation.getArgument(2, LongTokenRange.class);
                    RepairHistory.RepairSession repairSession = mock(RepairHistory.RepairSession.class);
                    repairSessions.put(range, repairSession);
                    return repairSession;
                });
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testRepairSuccessfully() throws InterruptedException
    {
        Set<LongTokenRange> ranges = new HashSet<>();
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);

        ranges.add(range1);
        ranges.add(range2);


        final VnodeRepairTask repairTask = new VnodeRepairTask(jmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, repairHistory, ranges, participants, jobId);

        CountDownLatch cdl = startRepair(repairTask, false, proxy);

        Notification notification = new Notification("progress", "repair:1", 0, getRepairMessage(range1));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 2));
        proxy.notify(notification);

        notification = new Notification("progress", "repair:1", 1, getRepairMessage(range2));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 2, 2));
        proxy.notify(notification);

        notification = new Notification("progress", "repair:1", 2, "Done with repair");
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.COMPLETE.ordinal(), 2, 2));
        proxy.notify(notification);

        cdl.await();

        assertThat(repairTask.getUnknownRanges()).isNull();
        assertThat(repairTask.getFailedRanges()).isEmpty();
        assertThat(repairTask.getSuccessfulRanges()).containsExactlyInAnyOrderElementsOf(ranges);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();
        assertThat(proxy.myOptions.get(RepairOptions.INCREMENTAL_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.COLUMNFAMILIES_KEY)).isEqualTo(myTableReference.getTable());
        assertThat(proxy.myOptions.get(RepairOptions.PRIMARY_RANGE_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.PARALLELISM_KEY)).isEqualTo(myRepairConfiguration.getRepairParallelism().getName());
        assertThat(proxy.myOptions.get(RepairOptions.HOSTS_KEY)).isEqualTo(participants.stream().map(host -> host.getPublicAddress().getHostAddress())
                .collect(Collectors.joining(",")));

        verify(myTableRepairMetrics).repairSession(eq(myTableReference), anyLong(), any(TimeUnit.class), eq(true));
        verify(repairSessions.get(range1)).start();
        verify(repairSessions.get(range2)).start();
        verify(repairSessions.get(range1)).finish(eq(RepairStatus.SUCCESS));
        verify(repairSessions.get(range2)).finish(eq(RepairStatus.SUCCESS));
    }

    @Test
    public void testRepairSingleRangeSuccessfully() throws InterruptedException
    {
        Set<LongTokenRange> ranges = new HashSet<>();
        LongTokenRange range = new LongTokenRange(1, 2);

        ranges.add(range);

        final VnodeRepairTask repairTask = new VnodeRepairTask(jmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, repairHistory, ranges, participants, jobId);

        CountDownLatch cdl = startRepair(repairTask, false, proxy);

        Notification notification = new Notification("progress", "repair:1", 0, getRepairMessage(range));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 2));
        proxy.notify(notification);

        notification = new Notification("progress", "repair:1", 1, "Done with repair");
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.COMPLETE.ordinal(), 2, 2));
        proxy.notify(notification);

        cdl.await();

        assertThat(repairTask.getUnknownRanges()).isNull();
        assertThat(repairTask.getFailedRanges()).isEmpty();
        assertThat(repairTask.getSuccessfulRanges()).containsExactlyInAnyOrderElementsOf(ranges);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();
        assertThat(proxy.myOptions.get(RepairOptions.INCREMENTAL_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.COLUMNFAMILIES_KEY)).isEqualTo(myTableReference.getTable());
        assertThat(proxy.myOptions.get(RepairOptions.PRIMARY_RANGE_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.PARALLELISM_KEY)).isEqualTo(myRepairConfiguration.getRepairParallelism().getName());
        assertThat(proxy.myOptions.get(RepairOptions.HOSTS_KEY)).isEqualTo(participants.stream().map(host -> host.getPublicAddress().getHostAddress())
                .collect(Collectors.joining(",")));

        verify(myTableRepairMetrics).repairSession(eq(myTableReference), anyLong(), any(TimeUnit.class), eq(true));
        verify(repairSessions.get(range)).start();
        verify(repairSessions.get(range)).finish(eq(RepairStatus.SUCCESS));
    }

    @Test
    public void testRepairHalf() throws InterruptedException
    {
        Set<LongTokenRange> ranges = new HashSet<>();
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);

        ranges.add(range1);
        ranges.add(range2);

        final VnodeRepairTask repairTask = new VnodeRepairTask(jmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, repairHistory, ranges, participants, jobId);

        CountDownLatch cdl = startRepair(repairTask, true, proxy);

        Notification notification = new Notification("progress", "repair:1", 0, getRepairMessage(range1));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 2));
        proxy.notify(notification);

        notification = new Notification(JMXConnectionNotification.FAILED, "repair:1", 2, "Failed repair");
        proxy.notify(notification);

        cdl.await();

        assertThat(repairTask.getUnknownRanges()).containsExactly(range2);
        assertThat(repairTask.getFailedRanges()).isEmpty();
        assertThat(repairTask.getSuccessfulRanges()).containsExactly(range1);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();
        assertThat(proxy.myOptions.get(RepairOptions.INCREMENTAL_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.COLUMNFAMILIES_KEY)).isEqualTo(myTableReference.getTable());
        assertThat(proxy.myOptions.get(RepairOptions.PRIMARY_RANGE_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.PARALLELISM_KEY)).isEqualTo(myRepairConfiguration.getRepairParallelism().getName());
        assertThat(proxy.myOptions.get(RepairOptions.HOSTS_KEY)).isEqualTo(participants.stream().map(host -> host.getPublicAddress().getHostAddress())
                .collect(Collectors.joining(",")));

        verify(myTableRepairMetrics).repairSession(eq(myTableReference), anyLong(), any(TimeUnit.class), eq(false));
        verify(repairSessions.get(range1)).start();
        verify(repairSessions.get(range2)).start();
        verify(repairSessions.get(range1)).finish(eq(RepairStatus.SUCCESS));
        verify(repairSessions.get(range2)).finish(eq(RepairStatus.FAILED));
    }

    @Test
    public void testPartialFailedRepair() throws InterruptedException
    {
        Set<LongTokenRange> ranges = new HashSet<>();
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);

        ranges.add(range1);
        ranges.add(range2);

        final VnodeRepairTask repairTask = new VnodeRepairTask(jmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, repairHistory, ranges, participants, jobId);

        CountDownLatch cdl = startRepair(repairTask, true, proxy);

        Notification notification = new Notification("progress", "repair:1", 0, getRepairMessage(range1));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 2));
        proxy.notify(notification);

        notification = new Notification("progress", "repair:1", 1, getFailedRepairMessage(range2));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 2, 2));
        proxy.notify(notification);

        notification = new Notification("progress", "repair:1", 2, "Done with repair");
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.COMPLETE.ordinal(), 2, 2));
        proxy.notify(notification);

        cdl.await();

        assertThat(repairTask.getUnknownRanges()).isNull();
        assertThat(repairTask.getFailedRanges()).containsExactly(range2);
        assertThat(repairTask.getSuccessfulRanges()).containsExactly(range1);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();
        assertThat(proxy.myOptions.get(RepairOptions.INCREMENTAL_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.COLUMNFAMILIES_KEY)).isEqualTo(myTableReference.getTable());
        assertThat(proxy.myOptions.get(RepairOptions.PRIMARY_RANGE_KEY)).isEqualTo("false");
        assertThat(proxy.myOptions.get(RepairOptions.PARALLELISM_KEY)).isEqualTo(myRepairConfiguration.getRepairParallelism().getName());
        assertThat(proxy.myOptions.get(RepairOptions.HOSTS_KEY)).isEqualTo(participants.stream().map(host -> host.getPublicAddress().getHostAddress())
                .collect(Collectors.joining(",")));

        verify(myTableRepairMetrics).repairSession(eq(myTableReference), anyLong(), any(TimeUnit.class), eq(false));
        verify(repairSessions.get(range1)).start();
        verify(repairSessions.get(range2)).start();
        verify(repairSessions.get(range1)).finish(eq(RepairStatus.SUCCESS));
        verify(repairSessions.get(range2)).finish(eq(RepairStatus.FAILED));
    }

    @Test
    public void testShouldMatchProgressNotificationPattern()
    {
        Set<LongTokenRange> ranges = new HashSet<>();
        LongTokenRange range = new LongTokenRange(1, 2);
        ranges.add(range);

        final VnodeRepairTask repairTask = new VnodeRepairTask(jmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, repairHistory, ranges, participants, jobId);

        repairTask.progress(ProgressEventType.PROGRESS, getRepairMessage(range));

        assertThat(repairTask.getSuccessfulRanges()).containsExactly(range);
        verify(repairSessions.get(range)).finish(eq(RepairStatus.SUCCESS));
    }

    private DriverNode mockNode()
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getId()).thenReturn(UUID.randomUUID());
        when(node.getPublicAddress()).thenReturn(InetAddress.getLoopbackAddress());
        return node;
    }
}
