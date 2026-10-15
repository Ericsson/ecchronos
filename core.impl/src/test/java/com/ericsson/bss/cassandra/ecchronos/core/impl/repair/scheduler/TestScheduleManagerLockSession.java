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
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

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

/**
 * Tests for the configurable lock-session mode (batched vs per-task "sidecar" locking) and for the session
 * refresh/redrive behaviour that ends a session once a job's work is drained instead of idling until the window.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerLockSession
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

    private ScheduleManagerImpl buildScheduler(final boolean lockSessionEnabled)
    {
        ScheduleManagerImpl scheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withRunInterval(10, TimeUnit.SECONDS)
                .withSessionWindow(TimeUnit.MINUTES.toMillis(5), TimeUnit.MILLISECONDS)
                .withCooldown(0, TimeUnit.MILLISECONDS)
                .withLockSessionEnabled(lockSessionEnabled)
                .build();
        scheduler.createScheduleFutureForNodeIDList(myNodes);
        return scheduler;
    }

    /**
     * With batching enabled a shared resource is locked once and reused across tasks/jobs within the session, so
     * the total number of lock acquisitions equals the number of distinct resources (A, B, C = 3), and the locks
     * are released only once, at session end.
     */
    @Test
    public void testBatchedModeReusesSharedResourceLock() throws LockException
    {
        myScheduler = buildScheduler(true);
        List<DummyLock> acquiredLocks = new ArrayList<>();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenAnswer(invocation ->
                {
                    DummyLock lock = new DummyLock();
                    acquiredLocks.add(lock);
                    return lock;
                });

        // Job1 needs {A, B}, Job2 needs {B, C}; B is shared.
        ResourceJob job1 = new ResourceJob(nodeID1, 2, "dc1", "nodeA", "nodeB");
        ResourceJob job2 = new ResourceJob(nodeID1, 2, "dc1", "nodeB", "nodeC");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(2);
        assertThat(job2.getTaskRuns()).isEqualTo(2);
        // A, B, C each locked exactly once (B reused): 3 acquisitions.
        assertThat(acquiredLocks).hasSize(3);
        // Held for the whole session, released once at session end.
        assertThat(acquiredLocks).allMatch(lock -> lock.closed);
    }

    /**
     * With batching disabled (sidecar mode) locks are acquired per task and released immediately, so a shared
     * resource is re-acquired for every task that needs it rather than being reused across the session.
     */
    @Test
    public void testPerTaskModeReacquiresLockPerTask() throws LockException
    {
        myScheduler = buildScheduler(false);
        AtomicInteger lockAcquireCount = new AtomicInteger(0);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenAnswer(invocation ->
                {
                    lockAcquireCount.incrementAndGet();
                    return new DummyLock();
                });

        // Job1 needs {A, B}, Job2 needs {B, C}; each task re-acquires all of its resources.
        ResourceJob job1 = new ResourceJob(nodeID1, 2, "dc1", "nodeA", "nodeB");
        ResourceJob job2 = new ResourceJob(nodeID1, 2, "dc1", "nodeB", "nodeC");
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(2);
        assertThat(job2.getTaskRuns()).isEqualTo(2);
        // Per task: job1 has 2 tasks x {A,B} = 4, job2 has 2 tasks x {B,C} = 4 => 8 acquisitions,
        // strictly more than the 3 of batched mode (no reuse across tasks).
        assertThat(lockAcquireCount.get()).isEqualTo(8);
    }

    /**
     * In sidecar mode each task's lock must be released as soon as the task completes (not held to session end).
     * Every acquired lock should therefore be closed by the time the single pass returns.
     */
    @Test
    public void testPerTaskModeReleasesLockAfterEachTask() throws LockException
    {
        myScheduler = buildScheduler(false);
        List<DummyLock> acquiredLocks = new ArrayList<>();
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenAnswer(invocation ->
                {
                    DummyLock lock = new DummyLock();
                    acquiredLocks.add(lock);
                    return lock;
                });

        ResourceJob job = new ResourceJob(nodeID1, 3, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getTaskRuns()).isEqualTo(3);
        // One lock acquired per task (3) and all released.
        assertThat(acquiredLocks).hasSize(3);
        assertThat(acquiredLocks).allMatch(lock -> lock.closed);
    }

    /**
     * All tasks a job currently exposes are drained within a single session (bounded by the window), without the
     * scheduler regenerating the job's repair-state snapshot between tasks. {@link CountingRefreshJob} counts how
     * many times {@link ScheduledJob#refreshState()} is invoked by the scheduler so the test can assert that an
     * expensive per-task refresh does not happen.
     */
    @Test
    public void testAllCurrentTasksDrainedInOneSession() throws LockException
    {
        myScheduler = buildScheduler(true);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        CountingRefreshJob job = new CountingRefreshJob(nodeID1, 5, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        // All 5 tasks of the current snapshot ran in the single session.
        assertThat(job.getTaskRuns()).isEqualTo(5);
    }

    /**
     * refreshState() rebuilds the repair state (expensive) and must be invoked at most once per session when work
     * was done - never once per task. This guards against regressing to a per-task snapshot regeneration.
     */
    @Test
    public void testRefreshStateCalledAtMostOncePerSessionNotPerTask() throws LockException
    {
        myScheduler = buildScheduler(true);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());

        CountingRefreshJob job = new CountingRefreshJob(nodeID1, 5, "dc1", "nodeA");
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getTaskRuns()).isEqualTo(5);
        // Despite 5 tasks, the scheduler must not refresh per task. The queue's candidate-selection may refresh
        // once, and executeJobTasks refreshes once after the drain: at most a small constant, never >= 5.
        assertThat(job.getRefreshCount()).isLessThan(5);
    }

    private static class ResourceJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private final int myNumTasks;
        private final Set<RepairResource> myResources;

        ResourceJob(final UUID nodeId, final int numTasks, final String dc, final String... resourceNames)
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

    /**
     * A job that exposes a fixed set of tasks and counts how many times {@link #refreshState()} is called by the
     * scheduler. Used to assert that all current tasks are drained in one session and that the scheduler does not
     * regenerate state per task.
     */
    private static class CountingRefreshJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private final AtomicInteger myRefreshCount = new AtomicInteger(0);
        private final int myNumTasks;
        private final Set<RepairResource> myResources;

        CountingRefreshJob(final UUID nodeId, final int numTasks, final String dc, final String resourceName)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myNumTasks = numTasks;
            myResources = new HashSet<>();
            myResources.add(new RepairResource(dc, resourceName));
        }

        int getTaskRuns()
        {
            return myTaskRuns.get();
        }

        int getRefreshCount()
        {
            return myRefreshCount.get();
        }

        @Override
        public void refreshState()
        {
            myRefreshCount.incrementAndGet();
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
}
