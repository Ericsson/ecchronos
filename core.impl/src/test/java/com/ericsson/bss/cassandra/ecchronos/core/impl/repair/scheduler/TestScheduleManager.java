/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactoryBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith (MockitoJUnitRunner.Silent.class)
public class TestScheduleManager
{
    @Mock
    private CASLockFactoryBuilder myLockFactoryBuilder;
    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private HostStates myHostStates;

    @Mock
    private RunPolicy myRunPolicy;

    @Mock
    private Node node1;

    @Mock
    private Node node2;

    private ScheduleManagerImpl myScheduler;

    private final UUID nodeID1 = UUID.randomUUID();

    private final UUID nodeID2 = UUID.randomUUID();

    private final Collection<UUID> myNodes = Arrays.asList(nodeID1, nodeID2);

    @Before
    public void startup() throws LockException
    {
        Map<UUID, Node> nodeMap = Map.of(nodeID1, node1, nodeID2, node2);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodeMap);
        myScheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .build();
        myScheduler.addRunPolicy((job, node) -> myRunPolicy.validate(job, node));
        myScheduler.createScheduleFutureForNodeIDList(myNodes);

        when(myRunPolicy.validate(any(ScheduledJob.class), any(Node.class))).thenReturn(-1L);
        doReturn(myLockFactoryBuilder).when(myLockFactoryBuilder).withNativeConnectionProvider(myNativeConnectionProvider);
        doReturn(myLockFactoryBuilder).when(myLockFactoryBuilder).withHostStates(myHostStates);
        doReturn(myLockFactory).when(myLockFactoryBuilder).build();
    }

    @After
    public void cleanup()
    {
        myScheduler.close();
    }

    @Test
    public void testRunningNoJobs() throws LockException
    {
        myScheduler.run(nodeID1);

        verify(myLockFactory, never()).tryLock(any(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testRunningOneJob()
    {
        DummyJob job1 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job1);

        myScheduler.run(nodeID1);

        assertThat(job1.hasRun()).isTrue();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    @Test
    public void testRunningJobWithFailingRunPolicy()
    {
        DummyJob job1 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job1);

        when(myRunPolicy.validate(any(ScheduledJob.class), any(Node.class))).thenReturn(1L);

        myScheduler.run(nodeID1);

        assertThat(job1.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    @Test
    public void testRunningTwoTasksStoppedAfterFirstByPolicy() throws LockException
    {
        TestJob job1 = new TestJob(ScheduledJob.Priority.LOW, 2, () -> {
            when(myRunPolicy.validate(any(ScheduledJob.class), any(Node.class))).thenReturn(1L);
        }, nodeID1);
        myScheduler.schedule(nodeID1, job1);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenReturn(new DummyLock());
        myScheduler.run(nodeID1);

        assertThat(job1.getTaskRuns()).isEqualTo(1);
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
        verify(myLockFactory).tryLock(any(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testRunningJobWithThrowingRunPolicy()
    {
        DummyJob job1 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job1);

        when(myRunPolicy.validate(any(ScheduledJob.class), any(Node.class))).thenThrow(new IllegalStateException());

        myScheduler.run(nodeID1);

        assertThat(job1.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    @Test
    public void testTwoJobsRejected()
    {
        DummyJob job1 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        when(myRunPolicy.validate(any(ScheduledJob.class), any(Node.class))).thenReturn(1L);

        myScheduler.run(nodeID1);

        assertThat(job1.hasRun()).isFalse();
        assertThat(job2.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(2);
        verify(myRunPolicy, times(2)).validate(any(ScheduledJob.class), any(Node.class));
    }

    @Test (timeout = 2000L)
    public void testDescheduleRunningJob() throws InterruptedException
    {
        CountDownLatch jobCdl = new CountDownLatch(1);
        TestJob job1 = new TestJob(ScheduledJob.Priority.HIGH, jobCdl, nodeID1);
        myScheduler.schedule(nodeID1, job1);

        new Thread(() -> myScheduler.run(nodeID1)).start();

        waitForJobStarted(job1);
        myScheduler.deschedule(nodeID1, job1);
        jobCdl.countDown();
        waitForJobFinished(job1);

        assertThat(job1.hasRun()).isTrue();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(0);
    }

    @Test
    public void testGetCurrentJobStatus() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        UUID jobId = UUID.randomUUID();
        ScheduledJob job1 = new TestScheduledJob(
                new ScheduledJob.ConfigurationBuilder()
                        .withPriority(ScheduledJob.Priority.LOW)
                        .withRunInterval(1, TimeUnit.SECONDS)
                        .build(),
                nodeID1,
                jobId,
                latch);
        myScheduler.schedule(nodeID1, job1);
        new Thread(() -> myScheduler.run(nodeID1)).start();
        Thread.sleep(50);
        assertThat(myScheduler.getCurrentJobStatus()).isEqualTo(jobId.toString());
        latch.countDown();
    }

    @Test
    public void testGetCurrentJobStatusNoRunning() throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        UUID jobId = UUID.randomUUID();
        ScheduledJob job1 = new TestScheduledJob(
                new ScheduledJob.ConfigurationBuilder()
                        .withPriority(ScheduledJob.Priority.LOW)
                        .withRunInterval(1, TimeUnit.SECONDS)
                        .build(),
                nodeID1,
                jobId,
                latch);
        myScheduler.schedule(nodeID1, job1);
        new Thread(() -> myScheduler.run(nodeID1)).start();
        assertThat(myScheduler.getCurrentJobStatus()).isEqualTo("");
        latch.countDown();
    }

    @Test
    public void testRunningOneJobWithThrowingLock() throws LockException
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenThrow(new LockException(""));

        myScheduler.run(nodeID1);

        assertThat(job.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    @Test
    public void testTwoJobsThrowingLock() throws LockException
    {
        DummyJob job1 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.LOW, nodeID1);
        myScheduler.schedule(nodeID1, job1);
        myScheduler.schedule(nodeID1, job2);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenThrow(new LockException(""));

        myScheduler.run(nodeID1);

        assertThat(job1.hasRun()).isFalse();
        assertThat(job2.hasRun()).isFalse();
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(2);
        verify(myLockFactory, times(2)).tryLock(any(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testThreeTasksOneThrowing() throws LockException
    {
        TestJob job = new TestJob(ScheduledJob.Priority.LOW, 3, nodeID1);
        myScheduler.schedule(nodeID1, job);

        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any()))
                .thenReturn(new DummyLock())
                .thenThrow(new LockException(""))
                .thenReturn(new DummyLock());

        myScheduler.run(nodeID1);

        assertThat(job.getTaskRuns()).isEqualTo(2);
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
        verify(myLockFactory, times(3)).tryLock(any(), anyString(), anyInt(), anyMap(), any());
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


        public TestJob(Priority priority, CountDownLatch cdl, UUID nodeId)
        {
            this(priority, cdl, 1, () -> {}, nodeId);
        }

        public TestJob(Priority priority, int numTasks, UUID nodeId)
        {
            this(priority, numTasks, () -> {}, nodeId);
        }

        public TestJob(Priority priority, int numTasks, Runnable onCompletion, UUID nodeId)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build(), nodeId);
            this.numTasks = numTasks;
            this.onCompletion = onCompletion;
        }

        public TestJob(Priority priority, CountDownLatch cdl, int numTasks, Runnable onCompletion, UUID nodeId)
        {
            super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build(), nodeId);
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
            public boolean execute(UUID nodeID)
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
        public TestScheduledJob(Configuration configuration, UUID nodeId, UUID jobId,
                CountDownLatch taskCompletionLatch)
        {
            super(configuration, jobId, nodeId);
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
            public boolean execute(UUID nodeID)
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
