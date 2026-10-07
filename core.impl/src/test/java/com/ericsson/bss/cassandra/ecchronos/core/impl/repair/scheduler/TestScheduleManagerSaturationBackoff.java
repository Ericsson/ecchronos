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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockClientSaturationException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerSaturationBackoff
{
    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node node1;

    private final UUID nodeID1 = UUID.randomUUID();
    private final Collection<UUID> myNodes = Collections.singletonList(nodeID1);

    private SimpleMeterRegistry myMeterRegistry;
    private ScheduleManagerImpl myScheduler;

    @Before
    public void startup()
    {
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(nodeID1, node1));
        myMeterRegistry = new SimpleMeterRegistry();
    }

    @After
    public void cleanup()
    {
        if (myScheduler != null)
        {
            myScheduler.close();
        }
    }

    private ScheduleManagerImpl buildScheduler()
    {
        ScheduleManagerImpl scheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withRunInterval(10, TimeUnit.SECONDS)
                .withSessionWindow(TimeUnit.MINUTES.toMillis(5), TimeUnit.MILLISECONDS)
                .withMeterRegistry(myMeterRegistry)
                .build();
        scheduler.createScheduleFutureForNodeIDList(myNodes);
        return scheduler;
    }

    private double saturationCount()
    {
        return myMeterRegistry.counter("ecc.lock.saturation.timeouts").count();
    }

    private double unavailableCount()
    {
        return myMeterRegistry.counter("ecc.lock.unavailable").count();
    }

    @Test
    public void testIsClientSaturationDetectsDirectAndWrapped()
    {
        assertThat(ScheduleManagerImpl.isClientSaturation(
                new LockClientSaturationException("timeout"))).isTrue();
        // Re-wrapped (as the lock cache does): a plain LockException with the saturation one as cause.
        assertThat(ScheduleManagerImpl.isClientSaturation(
                new LockException("Cached: timeout", new LockClientSaturationException("timeout")))).isTrue();
        // Ordinary contention/unavailability is not saturation.
        assertThat(ScheduleManagerImpl.isClientSaturation(
                new LockException("Not enough nodes available"))).isFalse();
        assertThat(ScheduleManagerImpl.isClientSaturation(null)).isFalse();
    }

    @Test
    public void testObservabilityMetricsRecordedOnSuccessfulPass() throws LockException
    {
        myScheduler = buildScheduler();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenReturn(new DummyLock());

        LockJob job = new LockJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        // The pass refreshed, acquired a lock successfully, and completed within the window.
        assertThat(myMeterRegistry.timer("ecc.scheduler.refresh").count()).isGreaterThanOrEqualTo(1L);
        assertThat(myMeterRegistry.counter("ecc.scheduler.lock.success").count()).isGreaterThanOrEqualTo(1.0d);
        assertThat(myMeterRegistry.timer("ecc.scheduler.lock.latency").count()).isGreaterThanOrEqualTo(1L);
        assertThat(myMeterRegistry.counter("ecc.scheduler.pass.within_window").count()).isGreaterThanOrEqualTo(1.0d);
        assertThat(job.getTaskRuns()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testLockFailureRecordsLockFailureMetric() throws LockException
    {
        myScheduler = buildScheduler();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockException("Not enough nodes available"));

        LockJob job = new LockJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(myMeterRegistry.counter("ecc.scheduler.lock.failure").count()).isGreaterThanOrEqualTo(1.0d);
    }

    @Test
    public void testSaturationTimeoutIncrementsSaturationMetricNotUnavailable() throws LockException
    {
        myScheduler = buildScheduler();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockClientSaturationException("client timeout"));

        LockJob job = new LockJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(saturationCount()).isEqualTo(1.0d);
        assertThat(unavailableCount()).isEqualTo(0.0d);
        assertThat(job.getTaskRuns()).isEqualTo(0);
    }

    @Test
    public void testUnavailabilityIncrementsUnavailableMetricNotSaturation() throws LockException
    {
        myScheduler = buildScheduler();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockException("Not enough nodes available"));

        LockJob job = new LockJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(unavailableCount()).isEqualTo(1.0d);
        assertThat(saturationCount()).isEqualTo(0.0d);
    }

    @Test
    public void testRepeatedSaturationTimeoutsKeepBackingOffAndCounting() throws LockException
    {
        myScheduler = buildScheduler();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockClientSaturationException("client timeout"));

        LockJob job = new LockJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        // First saturation failure sets a backoff and counts once.
        myScheduler.run(nodeID1);
        assertThat(myScheduler.isInContentionBackoff(job)).isTrue();
        assertThat(saturationCount()).isEqualTo(1.0d);

        // Clear the backoff window so the next pass attempts the lock again; a second consecutive saturation is
        // detected and counted (the escalating backoff grows with the consecutive count).
        myScheduler.clearContentionBackoffForTesting(job);
        myScheduler.run(nodeID1);
        assertThat(myScheduler.isInContentionBackoff(job)).isTrue();
        assertThat(saturationCount()).isEqualTo(2.0d);
    }

    private static final class LockJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private final int myNumTasks;
        private final Set<RepairResource> myResources;

        LockJob(final UUID nodeId, final int numTasks, final String dc, final String... resourceNames)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myNumTasks = numTasks;
            myResources = new HashSet<>();
            for (String name : resourceNames)
            {
                myResources.add(new RepairResource(dc, name));
            }
        }

        int getTaskRuns()
        {
            return myTaskRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            for (int i = 0; i < myNumTasks; i++)
            {
                tasks.add(new LockTask(myResources, myTaskRuns));
            }
            return tasks.iterator();
        }
    }

    private static final class LockTask extends ScheduledTask
    {
        private final Set<RepairResource> myResources;
        private final AtomicInteger myRunCounter;

        LockTask(final Set<RepairResource> resources, final AtomicInteger runCounter)
        {
            myResources = resources;
            myRunCounter = runCounter;
        }

        @Override
        public Set<RepairResource> getRepairResources()
        {
            return myResources;
        }

        @Override
        public TaskExecutionResult execute(final UUID nodeID)
        {
            myRunCounter.incrementAndGet();
            return TaskExecutionResult.SUCCESS;
        }
    }
}
