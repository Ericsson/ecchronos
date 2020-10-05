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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.management.Notification;
import javax.management.NotificationListener;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairGroupTasks
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";
    private static final TableReference tableReference = tableReference(keyspaceName, tableName);
    private static final int priority = 1;

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private LockFactory mockLockFactory;

    @Mock
    private JmxProxyFactory mockJmxProxyFactory;

    @Mock
    private TableRepairMetrics mockTableRepairMetrics;

    @Mock
    private RepairResourceFactory mockRepairResourceFactory;

    @Mock
    private RepairLockFactory mockRepairLockFactory;

    private RepairConfiguration repairConfiguration;

    @Before
    public void init()
    {
        repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();
    }

    @Test
    public void testExecute() throws Exception
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", keyspaceName);
        metadata.put("table", tableName);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(withHost("127.0.0.1")), ImmutableList.of(range(1, 2)));
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));

        when(mockJmxProxyFactory.connect()).thenReturn(new CustomJmxProxy((notificationListener, i) -> progressAndComplete(notificationListener, range(1, 2))));

        when(mockRepairResourceFactory.getRepairResources(eq(replicaRepairGroup))).thenReturn(repairResources);
        when(mockRepairLockFactory.getLock(eq(mockLockFactory), eq(repairResources), eq(metadata), eq(priority))).thenReturn(new DummyLock());

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, mockJmxProxyFactory, mockTableRepairMetrics,
                mockRepairResourceFactory, mockRepairLockFactory);

        assertThat(repairGroup.execute()).isTrue();
    }

    @Test (timeout = 1000L)
    public void testExecuteWithPolicyStoppingSecondTask() throws Exception
    {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("keyspace", keyspaceName);
        metadata.put("table", tableName);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(withHost("127.0.0.1")), ImmutableList.of(range(1, 2), range(2, 3)));
        Set<RepairResource> repairResources = Sets.newHashSet(new RepairResource("DC1", "my-resource"));
        final AtomicBoolean shouldRun = new AtomicBoolean(true);

        when(mockRepairResourceFactory.getRepairResources(eq(replicaRepairGroup))).thenReturn(repairResources);
        when(mockRepairLockFactory.getLock(eq(mockLockFactory), eq(repairResources), eq(metadata), eq(priority))).thenReturn(new DummyLock());

        TableRepairPolicy tableRepairPolicy = (tb) -> shouldRun.get();
        when(mockJmxProxyFactory.connect()).thenReturn(new CustomJmxProxy((notificationListener, i) -> {
            if (i == 1) // First repair
            {
                progressAndComplete(notificationListener, range(1, 2));
            }
            // After first repair task has completed we stop next task.
            // If this doesn't work a timeout will occur as the repair task
            // will be waiting for progress.
            shouldRun.set(false);
        }));

        RepairGroup repairGroup = new RepairGroup(priority, tableReference,
                repairConfiguration, replicaRepairGroup, mockJmxProxyFactory, mockTableRepairMetrics,
                mockRepairResourceFactory, mockRepairLockFactory,
                LongTokenRange.FULL_RANGE,
                Collections.singletonList(tableRepairPolicy));

        assertThat(repairGroup.execute()).isFalse();
    }

    private void progressAndComplete(NotificationListener notificationListener, LongTokenRange range)
    {
        // Normally the repair session id would be used here but
        // since we run this before repairAsync has completed we
        // have to use 0
        String repairSession = "repair:0";

        Notification notification = new Notification("progress", repairSession, 0, getRepairMessage(range));
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.PROGRESS.ordinal(), 1, 1));
        notificationListener.handleNotification(notification, null);

        notification = new Notification("progress", repairSession, 2, "Done with repair");
        notification.setUserData(getNotificationData(RepairTask.ProgressEventType.COMPLETE.ordinal(), 1, 1));
        notificationListener.handleNotification(notification, null);
    }

    private LongTokenRange range(long start, long end)
    {
        return new LongTokenRange(start, end);
    }

    private Host withHost(String ip) throws UnknownHostException
    {
        Host host = mock(Host.class);

        when(host.getBroadcastAddress()).thenReturn(InetAddress.getByName(ip));

        return host;
    }

    private String getRepairMessage(LongTokenRange range)
    {
        return String.format("Repair session RepairSession for range %s finished", Collections.singletonList(range));
    }

    private Map<String, Integer> getNotificationData(int type, int progressCount, int total)
    {
        Map<String, Integer> data = new HashMap<>();
        data.put("type", type);
        data.put("progressCount", progressCount);
        data.put("total", total);
        return data;
    }

    class CustomJmxProxy implements JmxProxy
    {
        private final AtomicInteger repairCount = new AtomicInteger();
        private final BiConsumer<NotificationListener, Integer> onRepair;

        private AtomicReference<NotificationListener> notificationListener = new AtomicReference<>();

        CustomJmxProxy(BiConsumer<NotificationListener, Integer> onRepair)
        {
            this.onRepair = onRepair;
        }

        @Override
        public void addStorageServiceListener(NotificationListener listener)
        {
            assertThat(notificationListener.compareAndSet(null, listener)).isTrue();
        }

        @Override
        public List<String> getLiveNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getUnreachableNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int repairAsync(String keyspace, Map<String, String> options)
        {
            int repair = repairCount.incrementAndGet();
            onRepair.accept(notificationListener.get(), repair);
            return repair;
        }

        @Override
        public void forceTerminateAllRepairSessions()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeStorageServiceListener(NotificationListener listener)
        {
            assertThat(notificationListener.compareAndSet(listener, null)).isTrue();
        }

        @Override
        public long liveDiskSpaceUsed(TableReference tableReference)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
