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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob.Priority;

public class TestScheduledJobQueue
{
    private ScheduledJobQueue queue;

    @Before
    public void setup()
    {
        queue = new ScheduledJobQueue(new Comp());
    }

    @Test
    public void testInsertRemoveOne()
    {
        DummyJob job = new DummyJob(Priority.LOW);

        queue.add(job);

        assertThat(queue.iterator()).containsExactly(job);
    }

    @Test
    public void testInsertDifferentPrio()
    {
        DummyJob job = new DummyJob(Priority.LOW);
        DummyJob job2 = new DummyJob(Priority.HIGH);

        queue.add(job);
        queue.add(job2);

        assertThat(queue.iterator()).containsExactly(job2, job);
    }

    @Test
    public void testEmptyQueue()
    {
        assertThat(queue.iterator()).isEmpty();
    }

    @Test
    public void testNonRunnableQueueIsEmpty() throws ScheduledJobException
    {
        final int nJobs = 10;

        for (int i = 0; i < nJobs; i++)
        {
            queue.add(new RunnableOnce(Priority.LOW));
        }

        for (ScheduledJob job : queue)
        {
            job.postExecute(true, null);
        }

        assertThat(queue.iterator()).isEmpty();
    }

    @Test
    public void testRemoveJobInQueueIsPossible()
    {
        DummyJob job = new DummyJob(Priority.HIGH);
        DummyJob job2 = new DummyJob(Priority.LOW);

        queue.add(job);
        queue.add(job2);

        Iterator<ScheduledJob> iterator = queue.iterator();

        queue.remove(job2);

        Assertions.assertThat(iterator).containsExactly(job, job2);
        assertThat(queue.iterator()).containsExactly(job);
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

        Assertions.assertThat(queue.iterator()).containsExactly(job2);
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

        Assertions.assertThat(queue.iterator()).containsExactly(job2);
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
            super(new ConfigurationBuilder().withPriority(prio).withRunInterval(1, TimeUnit.DAYS).build());
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
            super(priority);
            this.state = state;
        }

        @Override
        public State getState()
        {
            return state;
        }
    }
}
