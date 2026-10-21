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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
public class TestScheduleManagerSession
{
    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node node1;

    private final UUID nodeID1 = UUID.randomUUID();
    private final Collection<UUID> myNodes = Collections.singletonList(nodeID1);

    private ScheduleManagerImpl myScheduler;
    private final MutableClock myClock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));

    @Before
    public void startup()
    {
        Map<UUID, Node> nodeMap = Map.of(nodeID1, node1);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodeMap);
    }

    @After
    public void cleanup()
    {
        if (myScheduler != null)
        {
            myScheduler.close();
        }
    }

    private ScheduleManagerImpl buildScheduler(final long sessionWindowMs, final long cooldownMs)
    {
        ScheduleManagerImpl scheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withRunInterval(10, TimeUnit.SECONDS)
                .withSessionWindow(sessionWindowMs, TimeUnit.MILLISECONDS)
                .withCooldown(cooldownMs, TimeUnit.MILLISECONDS)
                // Drive session-window and cooldown timing from a controllable clock so tests are deterministic
                // and free of real sleeps. Virtual time only advances when a test (or a SlowTask) advances it.
                .withClock(myClock)
                .build();
        scheduler.createScheduleFutureForNodeIDList(myNodes);
        return scheduler;
    }

    @Test
    public void testMultipleJobsExecuteWithinSingleSession() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        ResourceJob job1 = new ResourceJob(nodeID1, 2, "dc1", "nodeA");
        ResourceJob job2 = new ResourceJob(nodeID1, 2, "dc1", "nodeB");
        ResourceJob job3 = new ResourceJob(nodeID1, 2, "dc1", "nodeC");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);
        myScheduler.schedule(nodeID1, job3);

        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(2);
        assertThat(job2.getTaskRuns()).isEqualTo(2);
        assertThat(job3.getTaskRuns()).isEqualTo(2);
    }

    @Test
    public void testLockResourceCompatibilityAcrossJobs() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 0);
        AtomicInteger lockAcquireCount = new AtomicInteger(0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenAnswer(invocation ->
                {
                    lockAcquireCount.incrementAndGet();
                    return new DummyLock();
                });

        // Job1 needs resources {A, B}, Job2 needs resources {B, C}
        // With shared session lock pool, B is reused from job1
        ResourceJob job1 = new ResourceJob(nodeID1, 2, "dc1", "nodeA", "nodeB");
        ResourceJob job2 = new ResourceJob(nodeID1, 2, "dc1", "nodeB", "nodeC");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(2);
        assertThat(job2.getTaskRuns()).isEqualTo(2);
        // Job1 acquires A and B (2 locks)
        // Job2 reuses B, only acquires C (1 lock)
        assertThat(lockAcquireCount.get()).isEqualTo(3);
    }

    @Test
    public void testLockFailureOnSecondJobSkipsTasksNotEntireSession() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenAnswer(invocation ->
                {
                    String resource = invocation.getArgument(1);
                    if (resource.contains("nodeB"))
                    {
                        throw new LockException("Resource busy");
                    }
                    return new DummyLock();
                });

        ResourceJob job1 = new ResourceJob(nodeID1, 2, "dc1", "nodeA");
        ResourceJob job2 = new ResourceJob(nodeID1, 2, "dc1", "nodeB");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        myScheduler.run(nodeID1);

        // Job1 succeeds (nodeA lock available)
        assertThat(job1.getTaskRuns()).isEqualTo(2);
        // Job2 tasks are skipped (nodeB lock unavailable)
        assertThat(job2.getTaskRuns()).isEqualTo(0);
    }

    @Test
    public void testCooldownPreventsImmediateRerun() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 5000);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        ResourceJob job1 = new ResourceJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job1);

        // First run executes the job
        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(1);

        // Second run immediately after should be blocked by cooldown
        job1.resetRuns();
        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(0);
    }

    @Test
    public void testCooldownExpiresAndAllowsRerun() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 2000);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        // A zero run-interval job is always immediately re-runnable, so the only thing gating a rerun is the
        // scheduler's cooldown. This isolates the behaviour under test and removes any dependence on real time
        // (ScheduledJob.getRealPriority compares against the real system clock, not the injected scheduler clock).
        ResourceJob job1 = new ResourceJob(nodeID1, 1, 0L, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job1);

        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(1);

        // Immediately after - cooldown blocks
        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(1);

        // Advance virtual time past the 2000ms cooldown deterministically (no real sleeping).
        myClock.advance(Duration.ofMillis(2001));

        // After cooldown, job should run again
        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(2);
    }

    @Test
    public void testSessionWindowTimeoutMidExecution() throws LockException
    {
        // Session window of 50ms; each task consumes 30ms of virtual time. The window is checked before each
        // task: task 1 at elapsed 0ms (<50, runs -> 30ms), task 2 at 30ms (<50, runs -> 60ms), task 3 at
        // 60ms (>=50, break). So exactly 2 of the 5 tasks run - a deterministic outcome, not a timing race.
        myScheduler = buildScheduler(50, 0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        SlowJob job = new SlowJob(nodeID1, 5, 30, myClock); // 5 tasks, 30ms each
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getTaskRuns()).isEqualTo(2);
    }

    @Test
    public void testSessionWindowTimeoutStopsSubsequentJobs() throws LockException
    {
        // Window 80ms; job1's single task consumes 100ms of virtual time, overrunning the window. The
        // between-jobs window check then fails deterministically, so job2 never runs.
        myScheduler = buildScheduler(80, 0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        SlowJob job1 = new SlowJob(nodeID1, 1, 100, myClock); // 1 task, 100ms (exceeds the 80ms window)
        ResourceJob job2 = new ResourceJob(nodeID1, 1, "dc1", "nodeB");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(1);
        assertThat(job2.getTaskRuns()).isEqualTo(0);
    }

    @Test
    public void testNoCooldownWhenNoTasksExecuted() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 5000);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockException("Resource busy"));

        ResourceJob job1 = new ResourceJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job1);

        // First run - lock fails, tasks skipped, no cooldown engaged
        myScheduler.run(nodeID1);
        assertThat(job1.getTaskRuns()).isEqualTo(0);

        // Replace job1 (which has backoff) with a fresh job
        doReturn(new DummyLock())
                .when(myLockFactory).tryLock(any(), anyString(), anyInt(), anyMap(), any());
        myScheduler.deschedule(nodeID1, job1);
        ResourceJob job2 = new ResourceJob(nodeID1, 1, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job2);

        // Run again - no cooldown was engaged so job2 executes immediately
        myScheduler.run(nodeID1);
        assertThat(job2.getTaskRuns()).isEqualTo(1);
    }

    @Test
    public void testJobWithNoResourcesStillExecutes() throws LockException
    {
        myScheduler = buildScheduler(TimeUnit.MINUTES.toMillis(5), 0);
        // Lock factory should not be called for jobs with no resources
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenThrow(new LockException("Should not be called"));

        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.hasRun()).isTrue();
    }

    // --- Test helpers ---

    private static class ResourceJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private final int myNumTasks;
        private final Set<RepairResource> myResources;

        ResourceJob(final UUID nodeId, final int numTasks, final String dc, final String... resourceNames)
        {
            this(nodeId, numTasks, 1L, dc, resourceNames);
        }

        ResourceJob(final UUID nodeId, final int numTasks, final long runIntervalMs, final String dc,
                final String... resourceNames)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(runIntervalMs, TimeUnit.MILLISECONDS)
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

        void resetRuns()
        {
            myTaskRuns.set(0);
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            for (int i = 0; i < myNumTasks; i++)
            {
                tasks.add(new ResourceTask(myResources, myTaskRuns));
            }
            return tasks.iterator();
        }
    }

    private static class ResourceTask extends ScheduledTask
    {
        private final Set<RepairResource> myResources;
        private final AtomicInteger myRunCounter;

        ResourceTask(final Set<RepairResource> resources, final AtomicInteger runCounter)
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

    private static class SlowJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private final int myNumTasks;
        private final long myTaskDurationMs;
        private final MutableClock myClock;

        SlowJob(final UUID nodeId, final int numTasks, final long taskDurationMs, final MutableClock clock)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.SECONDS)
                    .build(), nodeId);
            myNumTasks = numTasks;
            myTaskDurationMs = taskDurationMs;
            myClock = clock;
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
                tasks.add(new SlowTask(myTaskDurationMs, myTaskRuns, myClock));
            }
            return tasks.iterator();
        }
    }

    private static class SlowTask extends ScheduledTask
    {
        private final long myDurationMs;
        private final AtomicInteger myRunCounter;
        private final MutableClock myClock;

        SlowTask(final long durationMs, final AtomicInteger runCounter, final MutableClock clock)
        {
            myDurationMs = durationMs;
            myRunCounter = runCounter;
            myClock = clock;
        }

        @Override
        public TaskExecutionResult execute(final UUID nodeID)
        {
            // Simulate the task consuming wall-clock time by advancing virtual time instead of sleeping.
            // This makes session-window expiry deterministic: the scheduler's window check reads the same
            // clock, so the outcome depends only on arithmetic (durations vs window), not on real timing.
            myClock.advance(Duration.ofMillis(myDurationMs));
            myRunCounter.incrementAndGet();
            return TaskExecutionResult.TERMINAL;
        }
    }

    /**
     * A manually advanced {@link Clock} used to drive the scheduler's timing deterministically. All timing
     * decisions (session window, cooldown) read from this clock, so advancing it is equivalent to time passing
     * without any real sleeping.
     */
    private static final class MutableClock extends Clock
    {
        private volatile Instant myInstant;
        private final ZoneId myZone;

        MutableClock(final Instant initial)
        {
            myInstant = initial;
            myZone = ZoneOffset.UTC;
        }

        void advance(final Duration duration)
        {
            myInstant = myInstant.plus(duration);
        }

        @Override
        public ZoneId getZone()
        {
            return myZone;
        }

        @Override
        public Clock withZone(final ZoneId zone)
        {
            return this;
        }

        @Override
        public Instant instant()
        {
            return myInstant;
        }
    }
}
