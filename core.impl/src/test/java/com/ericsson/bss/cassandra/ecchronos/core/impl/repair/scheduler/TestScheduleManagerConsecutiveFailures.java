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
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class TestScheduleManagerConsecutiveFailures
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
    public void startup() throws LockException
    {
        Map<UUID, Node> nodeMap = Map.of(nodeID1, node1);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodeMap);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock());
        myScheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withSessionWindow(5, TimeUnit.MINUTES)
                .build();
        myScheduler.createScheduleFutureForNodeIDList(myNodes);
    }

    @After
    public void cleanup()
    {
        if (myScheduler != null)
        {
            myScheduler.close();
        }
    }

    @Test
    public void testJobMarkedFailedAfterConsecutiveTaskFailures()
    {
        // A job whose tasks always fail
        FailingJob job = new FailingJob(nodeID1, ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testJobNotMarkedFailedBelowThreshold()
    {
        // A job with fewer failing tasks than the threshold
        FailingJob job = new FailingJob(nodeID1, ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES - 1);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testSuccessfulTaskResetsFailureCounter()
    {
        // A job with 4 failures, then 1 success, then 4 more failures (total 9 tasks, never hits 5 consecutive)
        MixedJob job = new MixedJob(nodeID1, new boolean[]{
                false, false, false, false, // 4 failures
                true,                        // 1 success (resets counter)
                false, false, false, false   // 4 more failures
        });
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        // Job should NOT be marked failed because the success in the middle reset the counter
        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testJobMarkedFailedAfterSuccessAndThenConsecutiveFailures()
    {
        // A job with 1 success, then enough consecutive failures to hit the threshold
        boolean[] results = new boolean[1 + ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES];
        results[0] = true;
        for (int i = 1; i < results.length; i++)
        {
            results[i] = false;
        }
        MixedJob job = new MixedJob(nodeID1, results);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testFailedJobIsPurgedFromQueue()
    {
        FailingJob job = new FailingJob(nodeID1, ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);
        assertThat(job.getState()).isEqualTo(ScheduledJob.State.FAILED);

        // Run again — the queue iterator should purge the FAILED job
        myScheduler.run(nodeID1);
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(0);
    }

    @Test
    public void testFailureCounterClearedOnDeschedule()
    {
        // A job that partially fails (below threshold)
        FailingJob job = new FailingJob(nodeID1, 2);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);
        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);

        // Deschedule and reschedule — counter should be reset
        myScheduler.deschedule(nodeID1, job);

        // New job with same pattern — should start with fresh counter
        FailingJob job2 = new FailingJob(nodeID1, 2);
        myScheduler.schedule(nodeID1, job2);
        myScheduler.run(nodeID1);
        assertThat(job2.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
    }

    // --- ScheduledJob.markFailed() unit tests ---

    @Test
    public void testMarkFailedChangesState()
    {
        FailingJob job = new FailingJob(nodeID1, 1);
        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);

        job.markFailed();

        assertThat(job.getState()).isEqualTo(ScheduledJob.State.FAILED);
    }

    @Test
    public void testMarkFailedMakesJobNotRunnable()
    {
        FailingJob job = new FailingJob(nodeID1, 1);
        // Job should be runnable initially (configured with 1ms run interval)
        assertThat(job.runnable()).isTrue();

        job.markFailed();

        assertThat(job.runnable()).isFalse();
    }

    // --- Test helpers ---

    private static class FailingJob extends ScheduledJob
    {
        private final int myNumTasks;
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);

        FailingJob(final UUID nodeId, final int numTasks)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myNumTasks = numTasks;
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
                tasks.add(new FailingTask(myTaskRuns));
            }
            return tasks.iterator();
        }
    }

    private static class FailingTask extends ScheduledTask
    {
        private final AtomicInteger myRunCounter;

        FailingTask(final AtomicInteger runCounter)
        {
            myRunCounter = runCounter;
        }

        @Override
        public boolean execute(final UUID nodeID)
        {
            myRunCounter.incrementAndGet();
            return false;
        }
    }

    private static class MixedJob extends ScheduledJob
    {
        private final boolean[] myResults;

        MixedJob(final UUID nodeId, final boolean[] results)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myResults = results;
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            for (boolean result : myResults)
            {
                tasks.add(new ConfigurableTask(result));
            }
            return tasks.iterator();
        }
    }

    private static class ConfigurableTask extends ScheduledTask
    {
        private final boolean myResult;

        ConfigurableTask(final boolean result)
        {
            myResult = result;
        }

        @Override
        public boolean execute(final UUID nodeID)
        {
            return myResult;
        }
    }
}
