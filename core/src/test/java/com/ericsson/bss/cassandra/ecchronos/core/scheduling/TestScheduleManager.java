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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith (MockitoJUnitRunner.class)
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
        when(myLockFactory.tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class))).thenReturn(new DummyLock());
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

        verify(myLockFactory, never()).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));
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

        when(myLockFactory.tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class))).thenThrow(new LockException(""));

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test (timeout = 2000L)
    public void testRunningTwoJobsInParallelShouldFail() throws InterruptedException
    {
        LongRunningJob job = new LongRunningJob(ScheduledJob.Priority.HIGH);
        LongRunningJob job2 = new LongRunningJob(ScheduledJob.Priority.LOW);
        myScheduler.schedule(job);
        myScheduler.schedule(job2);

        final CountDownLatch cdl = new CountDownLatch(1);

        new Thread()
        {

            @Override
            public void run()
            {
                myScheduler.run();
                cdl.countDown();
            }
        }.start();

        myScheduler.run();

        cdl.await();

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

        when(myLockFactory.tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class))).thenThrow(new LockException(""));

        myScheduler.run();

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize()).isEqualTo(2);
        verify(myLockFactory, times(2)).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));
    }

    @Test
    public void testThreeTasksOneThrowing() throws LockException
    {
        ShortRunningMultipleTasks job = new ShortRunningMultipleTasks(ScheduledJob.Priority.LOW, 3);
        myScheduler.schedule(job);

        when(myLockFactory.tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class)))
                .thenReturn(new DummyLock())
                .thenThrow(new LockException(""))
                .thenReturn(new DummyLock());

        myScheduler.run();

        assertThat(job.getNumRuns()).isEqualTo(2);
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
        verify(myLockFactory, times(3)).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));
    }

    @Test (timeout = 2000L)
    public void testRemoveLongRunningJob() throws InterruptedException
    {
        LongRunningJob job = new LongRunningJob(ScheduledJob.Priority.HIGH);
        myScheduler.schedule(job);

        final CountDownLatch cdl = new CountDownLatch(1);

        new Thread()
        {
            @Override
            public void run()
            {
                myScheduler.run();
                cdl.countDown();
            }
        }.start();

        while(!job.hasStarted())
        {
            Thread.sleep(10);
        }

        myScheduler.deschedule(job);

        cdl.await();

        assertThat(job.hasRun()).isTrue();
        assertThat(myScheduler.getQueueSize()).isEqualTo(0);
    }

    private class LongRunningJob extends ScheduledJob
    {
        private volatile boolean hasRun = false;
        private volatile boolean hasStarted = false;

        public LongRunningJob(Priority priority)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build());
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
            return Arrays.<ScheduledTask> asList(new LongRunningTask()).iterator();
        }

        @Override
        public String toString()
        {
            return "LongRunningJob " + getPriority();
        }

        public class LongRunningTask extends ScheduledTask
        {
            @Override
            public boolean execute()
            {
                hasStarted = true;
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    // Intentionally left empty
                }
                hasRun = true;
                return true;
            }

            @Override
            public void cleanup()
            {
                // NOOP
            }
        }
    }

    private class ShortRunningMultipleTasks extends ScheduledJob
    {
        private final AtomicInteger numRuns = new AtomicInteger();
        private final int numTasks;

        public ShortRunningMultipleTasks(Priority priority, int numTasks)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build());
            this.numTasks = numTasks;
        }

        public int getNumRuns()
        {
            return numRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();

            for (int i = 0; i < numTasks; i++)
            {
                tasks.add(new ShortRunningTask());
            }

            return tasks.iterator();
        }

        private class ShortRunningTask extends ScheduledTask
        {
            @Override
            public boolean execute()
            {
                numRuns.incrementAndGet();
                return true;
            }
        }
    }
}
