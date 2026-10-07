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
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerRetry
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
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(nodeID1, node1));
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenReturn(new DummyLock());
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
    public void testRetryReAddedTaskDoesNotRunInSameSession()
    {
        // Concern 1: a job that re-adds a rebuilt task on a retryable failure (like the on-demand jobs) must not
        // have that rebuilt task run again within the same scheduler session; it is deferred to a later tick.
        RetryReaddingJob job = new RetryReaddingJob(nodeID1);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        // The original task ran exactly once; the task re-added during postExecute did not run this session.
        assertThat(job.totalRuns()).isEqualTo(1);
        // The rebuilt task is deferred to a later tick by the backoff (setRunnableIn), so the base job is PARKED
        // (in a backoff window) rather than FAILED or run-again.
        assertThat(job.getState()).isEqualTo(ScheduledJob.State.PARKED);
        assertThat(job.isInBackoff()).isTrue();
    }

    @Test
    public void testManyRetryableFailuresDoNotTripConsecutiveFailureBreaker()
    {
        // Concern 2: retryable results must not advance the scheduler's 5-consecutive-failure circuit breaker.
        // A base ScheduledJob does not fail from task results, so we assert it is never forced to FAILED even with
        // far more than MAX_CONSECUTIVE_TASK_FAILURES retryable tasks in a session.
        int retryableTasks = ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES * 2;
        RetryableTasksJob job = new RetryableTasksJob(nodeID1, retryableTasks);
        myScheduler.schedule(nodeID1, job);

        myScheduler.run(nodeID1);

        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
        assertThat(job.totalRuns()).isEqualTo(retryableTasks);
    }

    /** A job that mimics the on-demand retry: on a retryable postExecute it re-adds a fresh task to its live set. */
    private static final class RetryReaddingJob extends ScheduledJob
    {
        private final Map<ScheduledTask, Boolean> myTasks = new ConcurrentHashMap<>();
        private final AtomicInteger myRuns = new AtomicInteger(0);

        RetryReaddingJob(final UUID nodeId)
        {
            super(new ConfigurationBuilder().withPriority(Priority.LOW).withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myTasks.put(new CountingTask(myRuns, TaskExecutionResult.RETRYABLE), Boolean.TRUE);
        }

        int totalRuns()
        {
            return myRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            return myTasks.keySet().iterator();
        }

        @Override
        public void postExecute(final TaskExecutionResult result, final ScheduledTask task)
        {
            myTasks.remove(task);
            if (result.isRetryable())
            {
                // Re-add a fresh task into the live map during the scheduler's iteration, and defer via backoff.
                myTasks.put(new CountingTask(myRuns, TaskExecutionResult.SUCCESS), Boolean.TRUE);
                setRunnableIn(TimeUnit.MINUTES.toMillis(1));
            }
            super.postExecute(result, task);
        }
    }

    /** A job with a fixed number of always-retryable tasks. */
    private static final class RetryableTasksJob extends ScheduledJob
    {
        private final int myNumTasks;
        private final AtomicInteger myRuns = new AtomicInteger(0);

        RetryableTasksJob(final UUID nodeId, final int numTasks)
        {
            super(new ConfigurationBuilder().withPriority(Priority.LOW).withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
            myNumTasks = numTasks;
        }

        int totalRuns()
        {
            return myRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            for (int i = 0; i < myNumTasks; i++)
            {
                tasks.add(new CountingTask(myRuns, TaskExecutionResult.RETRYABLE));
            }
            return tasks.iterator();
        }
    }

    private static final class CountingTask extends ScheduledTask
    {
        private final AtomicInteger myRunCounter;
        private final TaskExecutionResult myResult;

        CountingTask(final AtomicInteger runCounter, final TaskExecutionResult result)
        {
            myRunCounter = runCounter;
            myResult = result;
        }

        @Override
        public TaskExecutionResult execute(final UUID nodeID)
        {
            myRunCounter.incrementAndGet();
            return myResult;
        }
    }
}
