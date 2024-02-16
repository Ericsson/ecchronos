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
package com.ericsson.bss.cassandra.ecchronos.core.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;

@RunWith (MockitoJUnitRunner.Silent.class)
public class TestScheduleManager
{
    @Mock
    private LockFactory myLockFactory;

    @Mock
    private RunPolicy myRunPolicy;

    private ScheduleManagerImpl myScheduler;

    @Before
    public void startup() throws LockException
    {
        myScheduler = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .build();
        myScheduler.addRunPolicy(job -> myRunPolicy.validate(job));

        when(myRunPolicy.validate(any(ScheduledJob.class))).thenReturn(-1L);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap())).thenReturn(new DummyLock());
    }

    @After
    public void cleanup()
    {
        myScheduler.close();
    }

    @Test
    public void testRunningNoJobs() throws LockException
    {
        myScheduler.run();

        verify(myLockFactory, never()).tryLock(any(), anyString(), anyInt(), anyMap());
    }

    @Test
    public void testRunningOneJob()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);

        myScheduler.run();

        assertThat(job.hasRun()).isTrue();
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test
    public void testRunningJobWithFailingRunPolicy()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);

        when(myRunPolicy.validate(any(ScheduledJob.class))).thenReturn(1L);

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test
    public void testRunningTwoTasksStoppedAfterFirstByPolicy() throws LockException
    {
        TestJob job = new TestJob(ScheduledJob.Priority.LOW, 2, () -> {
            when(myRunPolicy.validate(any(ScheduledJob.class))).thenReturn(1L);
        });
        myScheduler.schedule(job);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap())).thenReturn(new DummyLock());
        myScheduler.run();

        assertThat(job.getTaskRuns()).isEqualTo(1);
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
        verify(myLockFactory).tryLock(any(), anyString(), anyInt(), anyMap());
    }

    @Test
    public void testRunningJobWithThrowingRunPolicy()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);

        when(myRunPolicy.validate(any(ScheduledJob.class))).thenThrow(new IllegalStateException());

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test
    public void testRunningOneJobWithThrowingLock() throws LockException
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap())).thenThrow(new LockException(""));

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test (timeout = 2000L)
    public void testRunningTwoJobsInParallelShouldFail() throws InterruptedException
    {
        CountDownLatch job1Latch = new CountDownLatch(1);
        TestJob job = new TestJob(ScheduledJob.Priority.HIGH, job1Latch);
        CountDownLatch job2Latch = new CountDownLatch(1);
        TestJob job2 = new TestJob(ScheduledJob.Priority.LOW, job2Latch);
        myScheduler.schedule(job);
        myScheduler.schedule(job2);

        new Thread(() -> myScheduler.run()).start();
        new Thread(() -> myScheduler.run()).start();
        waitForJobStarted(job);
        job1Latch.countDown();
        job2Latch.countDown();
        waitForJobFinished(job);

        assertThat(job.hasRun()).isTrue();
        assertThat(job2.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(2);
    }

    @Test
    public void testTwoJobsRejected()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);
        myScheduler.schedule(job2);

        when(myRunPolicy.validate(any(ScheduledJob.class))).thenReturn(1L);

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(job2.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(2);
        verify(myRunPolicy, times(2)).validate(any(ScheduledJob.class));
    }

    @Test
    public void testTwoJobsThrowingLock() throws LockException
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);
        myScheduler.schedule(job2);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap())).thenThrow(new LockException(""));

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(job2.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(2);
        verify(myLockFactory, times(2)).tryLock(any(), anyString(), anyInt(), anyMap());
    }

    @Test
    public void testThreeTasksOneThrowing() throws LockException
    {
        TestJob job = new TestJob(ScheduledJob.Priority.LOW, 3);
        myScheduler.schedule(job);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap()))
                .thenReturn(new DummyLock())
                .thenThrow(new LockException(""))
                .thenReturn(new DummyLock());

        myScheduler.run();

        assertThat(job.getTaskRuns()).isEqualTo(2);
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
        verify(myLockFactory, times(3)).tryLock(any(), anyString(), anyInt(), anyMap());
    }

    @Test (timeout = 2000L)
    public void testDescheduleRunningJob() throws InterruptedException
    {
        CountDownLatch jobCdl = new CountDownLatch(1);
        TestJob job = new TestJob(ScheduledJob.Priority.HIGH, jobCdl);
        myScheduler.schedule(job);

        new Thread(() -> myScheduler.run()).start();

        waitForJobStarted(job);
        myScheduler.deschedule(job);
        jobCdl.countDown();
        waitForJobFinished(job);

        assertThat(job.hasRun()).isTrue();
        assertThat(myScheduler.getQueueSize()).isEqualTo(0);
    }

    @Test
    public void testGetCurrentJobStatus() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        UUID jobId = UUID.randomUUID();
        ScheduledJob testJob = new TestScheduledJob(
                new ScheduledJob.ConfigurationBuilder()
                        .withPriority(ScheduledJob.Priority.LOW)
                        .withRunInterval(1, TimeUnit.SECONDS)
                        .build(),
                jobId,
                latch);
        myScheduler.schedule(testJob);
        new Thread(() -> myScheduler.run()).start();
        Thread.sleep(100);
        assertThat(myScheduler.getCurrentJobStatus()).isEqualTo("Job ID: " + jobId.toString() + ", Status: Running");
        latch.countDown();

    }
    private void waitForJobStarted(TestJob job) throws InterruptedException
    {
        while(!job.hasStarted())
        {
            Thread.sleep(10);
        }
    }

    private void waitForJobFinished(TestJob job) throws InterruptedException
    {
        while(!job.hasRun())
        {
            Thread.sleep(10);
        }
    }

    private class TestJob extends ScheduledJob
    {
        private volatile CountDownLatch countDownLatch;
        private volatile boolean hasRun = false;
        private volatile boolean hasStarted = false;
        private final AtomicInteger taskRuns = new AtomicInteger();
        private final int numTasks;
        private final Runnable onCompletion;


        public TestJob(Priority priority, CountDownLatch cdl)
        {
            this(priority, cdl, 1, () -> {});
        }

        public TestJob(Priority priority, int numTasks)
        {
            this(priority, numTasks, () -> {});
        }

        public TestJob(Priority priority, int numTasks, Runnable onCompletion)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build());
            this.numTasks = numTasks;
            this.onCompletion = onCompletion;
        }

        public TestJob(Priority priority, CountDownLatch cdl, int numTasks, Runnable onCompletion)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build());
            this.numTasks = numTasks;
            this.onCompletion = onCompletion;
            countDownLatch = cdl;
        }

        public int getTaskRuns()
        {
            return taskRuns.get();
        }

        public boolean hasStarted()
        {
            return hasStarted;
        }

        public boolean hasRun()
        {
            return hasRun;
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();

            for (int i = 0; i < numTasks; i++)
            {
                tasks.add(new ShortRunningTask(onCompletion));
            }

            return tasks.iterator();
        }

        private class ShortRunningTask extends ScheduledTask
        {
            private final Runnable onCompletion;

            public ShortRunningTask(Runnable onCompletion)
            {
                this.onCompletion = onCompletion;
            }

            @Override
            public boolean execute()
            {
                hasStarted = true;
                try
                {
                    if (countDownLatch != null)
                    {
                        countDownLatch.await();
                    }
                }
                catch (InterruptedException e)
                {
                    // Intentionally left empty
                }
                onCompletion.run();
                taskRuns.incrementAndGet();
                hasRun = true;
                return true;
            }
        }
    }

    public class TestScheduledJob extends ScheduledJob
    {
        private final CountDownLatch taskCompletionLatch;
        public TestScheduledJob(Configuration configuration, UUID id, CountDownLatch taskCompletionLatch)
        {
            super(configuration, id);
            this.taskCompletionLatch = taskCompletionLatch;
        }
        @Override
        public Iterator<ScheduledTask> iterator()
        {
            return Collections.<ScheduledTask> singleton(new ControllableTask(taskCompletionLatch)).iterator();
        }
        class ControllableTask extends ScheduledTask
        {
            private final CountDownLatch latch;
            public ControllableTask(CountDownLatch latch)
            {
                this.latch = latch;
            }
            @Override
            public boolean execute()
            {
                try
                {
                    latch.await();
                    return true;
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
    }
}
