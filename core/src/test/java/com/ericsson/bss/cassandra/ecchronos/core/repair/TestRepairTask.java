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
import static org.awaitility.Awaitility.await;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairTask.ProgressEventType;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairTask
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";

    private static final TableReference TABLE_REFERENCE = new TableReference(KEYSPACE_NAME, TABLE_NAME);

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private MockedJmxProxy proxy = new MockedJmxProxy(KEYSPACE_NAME, TABLE_NAME);

    private final TableReference myTableReference = new TableReference(KEYSPACE_NAME, TABLE_NAME);

    @Before
    public void setup() throws IOException
    {
        when(jmxProxyFactory.connect()).thenReturn(proxy);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testRepairSuccessfully() throws InterruptedException
    {
        Collection<LongTokenRange> ranges = new ArrayList<>();
        Collection<LongTokenRange> range1 = Collections.singleton(new LongTokenRange(1, 2));
        Collection<LongTokenRange> range2 = Collections.singleton(new LongTokenRange(3, 4));

        ranges.addAll(range1);
        ranges.addAll(range2);

        final RepairTask repairTask = new RepairTask.Builder()
                .withJMXProxyFactory(jmxProxyFactory)
                .withTableReference(myTableReference)
                .withTokenRanges(ranges)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        CountDownLatch cdl = startRepair(repairTask, false);

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
        assertThat(repairTask.getCompletedRanges()).containsExactlyElementsOf(ranges);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();

        verify(myTableRepairMetrics).repairTiming(eq(TABLE_REFERENCE), anyLong(), any(TimeUnit.class), eq(true));
    }

    @Test
    public void testRepairHalf() throws InterruptedException
    {
        Collection<LongTokenRange> ranges = new ArrayList<>();
        Collection<LongTokenRange> range1 = Collections.singleton(new LongTokenRange(1, 2));
        Collection<LongTokenRange> range2 = Collections.singleton(new LongTokenRange(3, 4));

        ranges.addAll(range1);
        ranges.addAll(range2);

        final RepairTask repairTask = new RepairTask.Builder()
                .withJMXProxyFactory(jmxProxyFactory)
                .withTableReference(myTableReference)
                .withTokenRanges(ranges)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        CountDownLatch cdl = startRepair(repairTask, true);

        Notification notification = new Notification("progress", "repair:1", 0, getRepairMessage(range1));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 2));
        proxy.notify(notification);

        notification = new Notification(JMXConnectionNotification.FAILED, "repair:1", 2, "Failed repair");
        proxy.notify(notification);

        cdl.await();

        assertThat(repairTask.getUnknownRanges()).containsExactlyElementsOf(range2);
        assertThat(repairTask.getCompletedRanges()).containsExactlyElementsOf(range1);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();

        verify(myTableRepairMetrics).repairTiming(eq(TABLE_REFERENCE), anyLong(), any(TimeUnit.class), eq(false));
    }

    @Test
    public void testPartialFailedRepair() throws InterruptedException
    {
        Collection<LongTokenRange> ranges = new ArrayList<>();
        Collection<LongTokenRange> range1 = Collections.singleton(new LongTokenRange(1, 2));
        Collection<LongTokenRange> range2 = Collections.singleton(new LongTokenRange(3, 4));

        ranges.addAll(range1);
        ranges.addAll(range2);

        final RepairTask repairTask = new RepairTask.Builder()
                .withJMXProxyFactory(jmxProxyFactory)
                .withTableReference(myTableReference)
                .withTokenRanges(ranges)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        CountDownLatch cdl = startRepair(repairTask, true);

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

        assertThat(repairTask.getUnknownRanges()).containsExactlyElementsOf(range2);
        assertThat(repairTask.getCompletedRanges()).containsExactlyElementsOf(range1);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();

        verify(myTableRepairMetrics).repairTiming(eq(TABLE_REFERENCE), anyLong(), any(TimeUnit.class), eq(false));
    }

    @Test
    public void testPartialRepair() throws InterruptedException
    {
        Collection<LongTokenRange> ranges = new ArrayList<>();
        Collection<LongTokenRange> range1 = Collections.singleton(new LongTokenRange(1, 2));
        Collection<LongTokenRange> range2 = Collections.singleton(new LongTokenRange(3, 4));

        ranges.addAll(range1);
        ranges.addAll(range2);

        final RepairTask repairTask = new RepairTask.Builder()
                .withJMXProxyFactory(jmxProxyFactory)
                .withTableReference(myTableReference)
                .withTokenRanges(ranges)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        CountDownLatch cdl = startRepair(repairTask, false);

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
        assertThat(repairTask.getCompletedRanges()).containsExactlyElementsOf(ranges);
        assertThat(proxy.myOptions.get(RepairOptions.RANGES_KEY)).isNotEmpty();

        verify(myTableRepairMetrics).repairTiming(eq(TABLE_REFERENCE), anyLong(), any(TimeUnit.class), eq(true));
    }

    @Test
    public void testShouldMatchProgressNotificationPattern()
    {
        Collection<LongTokenRange> ranges = Collections.singleton(new LongTokenRange(1, 2));

        final RepairTask repairTask = new RepairTask.Builder()
                .withJMXProxyFactory(jmxProxyFactory)
                .withTableReference(myTableReference)
                .withTokenRanges(ranges)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();

        repairTask.progress(ProgressEventType.PROGRESS, 1, 1, getRepairMessage(ranges));

        assertThat(repairTask.getCompletedRanges()).containsExactlyElementsOf(ranges);
    }

    private CountDownLatch startRepair(final RepairTask repairTask, final boolean assertFailed)
    {
        final CountDownLatch cdl = new CountDownLatch(1);

        new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    repairTask.execute();
                    assertThat(assertFailed).isFalse();
                }
                catch (ScheduledJobException e)
                {
                    // Intentionally left empty
                }
                finally
                {
                    cdl.countDown();
                }
            }
        }.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS).until(() -> proxy.myListener != null);

        return cdl;
    }

    private String getFailedRepairMessage(Collection<LongTokenRange> range)
    {
        return String.format("Repair session RepairSession for range %s failed with error ...", range);
    }

    private String getRepairMessage(Collection<LongTokenRange> range)
    {
        return String.format("Repair session RepairSession for range %s finished", range);
    }

    private Map<String, Integer> getNotificationData(int type, int progressCount, int total)
    {
        Map<String, Integer> data = new HashMap<>();
        data.put("type", type);
        data.put("progressCount", progressCount);
        data.put("total", total);
        return data;
    }

    public class MockedJmxProxy implements JmxProxy
    {
        public final String myKeyspace;
        public final String myTable;

        public volatile NotificationListener myListener;

        public volatile Map<String, String> myOptions;

        public MockedJmxProxy(String keyspace, String table)
        {
            myKeyspace = keyspace;
            myTable = table;
        }

        @Override
        public void close() throws IOException
        {
            // Intentionally left empty
        }

        @Override
        public void addStorageServiceListener(NotificationListener listener)
        {
            myListener = listener;
        }

        @Override
        public int repairAsync(String keyspace, Map<String, String> options)
        {
            myOptions = options;
            return 1;
        }

        @Override
        public void forceTerminateAllRepairSessions()
        {
            // NOOP
        }

        @Override
        public void removeStorageServiceListener(NotificationListener listener)
        {
            myListener = null;
        }

        @Override
        public long liveDiskSpaceUsed(TableReference tableReference)
        {
            return 0;
        }

        public void notify(Notification notification)
        {
            myListener.handleNotification(notification, null);
        }

        @Override
        public List<String> getLiveNodes()
        {
            return Collections.emptyList();
        }

        @Override
        public List<String> getUnreachableNodes()
        {
            return Collections.emptyList();
        }

    }
}
