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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestScheduledJobQueue
{
    private ScheduledJobQueue queue;
    private UUID nodeId = UUID.randomUUID();

    @Before
    public void setup()
    {
        queue = new ScheduledJobQueue(new Comp());
    }

    @Test
    public void testInsertRemoveOne()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW, nodeId);

        queue.add(job);

        assertThat(queue.iterator()).toIterable().containsExactly(job);
    }

    @Test
    public void testInsertDifferentPrio()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.LOW, nodeId);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.HIGH, nodeId);

        queue.add(job);
        queue.add(job2);

        assertThat(queue.iterator()).toIterable().containsExactly(job2, job);
    }

    @Test
    public void testEmptyQueue()
    {
        assertThat(queue.iterator()).toIterable().isEmpty();
    }

    @Test
    public void testNonRunnableQueueIsEmpty() throws ScheduledJobException
    {
        final int nJobs = 10;

        for (int i = 0; i < nJobs; i++)
        {
            queue.add(new RunnableOnce(ScheduledJob.Priority.LOW));
        }

        for (ScheduledJob job : queue)
        {
            job.postExecute(true, null);
        }

        assertThat(queue.iterator()).toIterable().isEmpty();
    }

    @Test
    public void testRemoveJobInQueueIsPossible()
    {
        DummyJob job = new DummyJob(ScheduledJob.Priority.HIGH, nodeId);
        DummyJob job2 = new DummyJob(ScheduledJob.Priority.LOW, nodeId);

        queue.add(job);
        queue.add(job2);

        Iterator<ScheduledJob> iterator = queue.iterator();

        queue.remove(job2);

        assertThat(iterator).toIterable().containsExactly(job, job2);
        assertThat(queue.iterator()).toIterable().containsExactly(job);
    }

    @Test
    public void testRunOnceJobRemovedOnFinish()
    {
        StateJob job = new StateJob(ScheduledJob.Priority.LOW, ScheduledJob.State.FINISHED);
        StateJob job2 = new StateJob(ScheduledJob.Priority.LOW, ScheduledJob.State.RUNNABLE);

        queue.add(job);
        queue.add(job2);

        for (ScheduledJob next : queue)
        {
            assertThat(next.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        }

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.iterator()).toIterable().containsExactly(job2);
    }

    @Test
    public void testRunOnceJobRemovedOnFailure()
    {
        StateJob job = new StateJob(ScheduledJob.Priority.LOW, ScheduledJob.State.FAILED);
        StateJob job2 = new StateJob(ScheduledJob.Priority.LOW, ScheduledJob.State.RUNNABLE);

        queue.add(job);
        queue.add(job2);

        for (ScheduledJob next : queue)
        {
            assertThat(next.getState()).isEqualTo(ScheduledJob.State.RUNNABLE);
        }

        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.iterator()).toIterable().containsExactly(job2);
    }

    @Test
    public void testRefreshPredicateSkipsExcludedJobs()
    {
        RefreshCountingJob refreshed = new RefreshCountingJob(ScheduledJob.Priority.LOW);
        RefreshCountingJob skipped = new RefreshCountingJob(ScheduledJob.Priority.HIGH);

        queue.add(refreshed);
        queue.add(skipped);

        // Only refresh 'refreshed'; 'skipped' must not have refreshState() invoked.
        Iterator<ScheduledJob> iterator = queue.iterator(job -> job == refreshed);
        // drain
        while (iterator.hasNext())
        {
            iterator.next();
        }

        assertThat(refreshed.refreshCount).isEqualTo(1);
        assertThat(skipped.refreshCount).isEqualTo(0);
    }

    @Test
    public void testRefreshPredicateAllRefreshesEveryJob()
    {
        RefreshCountingJob job1 = new RefreshCountingJob(ScheduledJob.Priority.LOW);
        RefreshCountingJob job2 = new RefreshCountingJob(ScheduledJob.Priority.HIGH);

        queue.add(job1);
        queue.add(job2);

        Iterator<ScheduledJob> iterator = queue.iterator(job -> true);
        while (iterator.hasNext())
        {
            iterator.next();
        }

        assertThat(job1.refreshCount).isEqualTo(1);
        assertThat(job2.refreshCount).isEqualTo(1);
    }

    @Test
    public void testNoArgIteratorRefreshesEveryJob()
    {
        RefreshCountingJob job1 = new RefreshCountingJob(ScheduledJob.Priority.LOW);
        RefreshCountingJob job2 = new RefreshCountingJob(ScheduledJob.Priority.HIGH);

        queue.add(job1);
        queue.add(job2);

        Iterator<ScheduledJob> iterator = queue.iterator();
        while (iterator.hasNext())
        {
            iterator.next();
        }

        assertThat(job1.refreshCount).isEqualTo(1);
        assertThat(job2.refreshCount).isEqualTo(1);
    }

    @Test
    public void testSkippedJobIsStillSelectableUsingItsExistingState()
    {
        // A job excluded from refresh is not removed from the queue; it is still returned by the iterator.
        RefreshCountingJob refreshed = new RefreshCountingJob(ScheduledJob.Priority.HIGH);
        RefreshCountingJob skipped = new RefreshCountingJob(ScheduledJob.Priority.LOW);

        queue.add(refreshed);
        queue.add(skipped);

        Iterator<ScheduledJob> iterator = queue.iterator(job -> job == refreshed);

        assertThat(iterator).toIterable().containsExactly(refreshed, skipped);
        assertThat(skipped.refreshCount).isEqualTo(0);
    }

    private class Comp implements Comparator<ScheduledJob>
    {

        @Override
        public int compare(ScheduledJob j1, ScheduledJob j2)
        {
            int ret = Integer.compare(j2.getRealPriority(), j1.getRealPriority());

            if (ret == 0)
            {
                ret = Integer.compare(j2.getPriority().getValue(), j1.getPriority().getValue());
            }

            return ret;
        }

    }

    private class RunnableOnce extends ScheduledJob
    {
        public RunnableOnce(Priority prio)
        {
            super(new ConfigurationBuilder().withPriority(prio).withRunInterval(1, TimeUnit.DAYS).build(), nodeId);
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            return new ArrayList<ScheduledTask>().iterator();
        }

        @Override
        public String toString()
        {
            return "RunnableOnce " + getPriority();
        }
    }

    private class StateJob extends DummyJob
    {
        private State state;
        StateJob(Priority priority, State state)
        {
            super(priority, nodeId);
            this.state = state;
        }

        @Override
        public State getState()
        {
            return state;
        }
    }

    private class RefreshCountingJob extends DummyJob
    {
        private int refreshCount = 0;

        RefreshCountingJob(Priority priority)
        {
            super(priority, nodeId);
        }

        @Override
        public void refreshState()
        {
            refreshCount++;
        }

        @Override
        public String toString()
        {
            return "RefreshCountingJob " + getPriority();
        }
    }
}
