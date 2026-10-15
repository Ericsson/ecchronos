/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Verifies the startup per-node stagger (#1897): the first {@code JobRunTask} run of each managed node is spread
 * across {@code [interval, 2*interval)} so no two nodes fire their first pass at the same instant, while the
 * runtime-add path keeps the plain {@code interval} delay.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerStartupStagger
{
    private static final long INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;

    private ScheduleManagerImpl myScheduler;

    @After
    public void cleanup()
    {
        if (myScheduler != null)
        {
            myScheduler.close();
        }
    }

    @Test
    public void testStartupDelaysAreStaggeredWithinOneInterval() throws Exception
    {
        int nodeCount = 6;
        List<UUID> nodes = newNodes(nodeCount);
        myScheduler = newScheduler(nodes);

        myScheduler.createScheduleFutureForNodeIDList(nodes);

        List<Long> delays = new ArrayList<>();
        for (UUID node : nodes)
        {
            long delay = scheduledDelayMs(node);
            // Each first run must be at least one full interval out (allowing the few ms between scheduling and
            // reading getDelay) and strictly below two intervals.
            assertThat(delay)
                    .as("startup delay for node %s", node)
                    .isGreaterThanOrEqualTo(INTERVAL_MS - toleranceMs())
                    .isLessThan(2 * INTERVAL_MS);
            delays.add(roundToStep(delay, nodeCount));
        }

        // All per-node (step-rounded) delays must be distinct: no two nodes aligned.
        assertThat(delays).doesNotHaveDuplicates();
        // Node 0 keeps the historical "first tick after one interval" behaviour.
        assertThat(delays.get(0)).isEqualTo(INTERVAL_MS);
    }

    @Test
    public void testRuntimeAddUsesPlainInterval() throws Exception
    {
        List<UUID> nodes = newNodes(1);
        myScheduler = newScheduler(nodes);

        UUID added = UUID.randomUUID();
        myScheduler.createScheduleFutureForNode(added);

        // The runtime-add path is unchanged: a plain one-interval delay, no stagger.
        assertThat(scheduledDelayMs(added))
                .isGreaterThanOrEqualTo(INTERVAL_MS - toleranceMs())
                .isLessThanOrEqualTo(INTERVAL_MS);
    }

    private ScheduleManagerImpl newScheduler(final List<UUID> nodes) throws LockException
    {
        Map<UUID, Node> nodeMap = new HashMap<>();
        for (UUID node : nodes)
        {
            nodeMap.put(node, org.mockito.Mockito.mock(Node.class));
        }
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodeMap);
        return ScheduleManagerImpl.builder()
                .withNodeIDList(nodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withRunInterval(INTERVAL_MS, TimeUnit.MILLISECONDS)
                .build();
    }

    private static List<UUID> newNodes(final int count)
    {
        List<UUID> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            nodes.add(UUID.randomUUID());
        }
        return nodes;
    }

    /**
     * Round a measured delay to its nominal per-node step so tiny measurement skew (the time between scheduling and
     * reading {@code getDelay}) does not make two adjacent steps appear distinct or equal by accident.
     */
    private static long roundToStep(final long delayMs, final int nodeCount)
    {
        long step = INTERVAL_MS / nodeCount;
        long offset = delayMs - INTERVAL_MS;
        return INTERVAL_MS + Math.round((double) offset / step) * step;
    }

    private static long toleranceMs()
    {
        return 1000L;
    }

    @SuppressWarnings("unchecked")
    private long scheduledDelayMs(final UUID nodeID) throws Exception
    {
        Field field = ScheduleManagerImpl.class.getDeclaredField("myRunFuture");
        field.setAccessible(true);
        Map<UUID, ScheduledFuture<?>> futures = (Map<UUID, ScheduledFuture<?>>) field.get(myScheduler);
        Delayed future = futures.get(nodeID);
        assertThat(future).as("scheduled future for node %s", nodeID).isNotNull();
        return future.getDelay(TimeUnit.MILLISECONDS);
    }
}
