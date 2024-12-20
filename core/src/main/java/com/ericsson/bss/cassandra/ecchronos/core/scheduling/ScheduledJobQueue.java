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

import java.util.Comparator;
import java.util.Queue;
import java.util.Map;
import java.util.EnumMap;
import java.util.PriorityQueue;
import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.utils.ManyToOneIterator;
import com.google.common.annotations.VisibleForTesting;

/**
 * Dynamic priority queue for scheduled jobs.
 * <p>
 * This queue is divided in several smaller queues, one for each {@link ScheduledJob.Priority priority type} and are
 * then retrieved using a
 * {@link ManyToOneIterator}.
 */
public class ScheduledJobQueue implements Iterable<ScheduledJob>
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJobQueue.class);

    private final Comparator<ScheduledJob> myComparator;

    private final Map<ScheduledJob.Priority, Queue<ScheduledJob>> myJobQueues
            = new EnumMap<>(ScheduledJob.Priority.class);

    /**
     * Construct a new job queue that prioritizes the jobs based on the provided comparator.
     *
     * @param comparator
     *            The comparator used to determine the job with the highest priority.
     */
    public ScheduledJobQueue(final Comparator<ScheduledJob> comparator)
    {
        this.myComparator = comparator;

        for (ScheduledJob.Priority priority : ScheduledJob.Priority.values())
        {
            myJobQueues.put(priority, new PriorityQueue<>(1, comparator));
        }
    }

    /**
     * Add a job to the queue.
     *
     * @param job
     *            The job to add.
     */
    public synchronized void add(final ScheduledJob job)
    {
        addJobInternal(job);
    }

    /**
     * Add a collection of jobs to the queue at once.
     *
     * @param jobs
     *            The collection of jobs.
     */
    public synchronized void addAll(final Collection<? extends ScheduledJob> jobs)
    {
        for (ScheduledJob job : jobs)
        {
            addJobInternal(job);
        }
    }

    /**
     * Remove the provided job from the queue.
     *
     * @param job
     *            The job to remove.
     */
    public synchronized void remove(final ScheduledJob job)
    {
        LOG.debug("Removing job: {}", job);
        myJobQueues.get(job.getPriority()).remove(job);
    }

    private void addJobInternal(final ScheduledJob job)
    {
        LOG.debug("Adding job: {}, Priority: {}", job, job.getPriority());
        myJobQueues.get(job.getPriority()).add(job);
    }

    @VisibleForTesting
    final int size()
    {
        int size = 0;

        for (Queue<ScheduledJob> queue : myJobQueues.values())
        {
            size += queue.size();
        }

        return size;
    }

    @Override
    public final synchronized Iterator<ScheduledJob> iterator()
    {
        myJobQueues.values().forEach(q -> q.forEach(ScheduledJob::refreshState));
        Iterator<ScheduledJob> baseIterator = new ManyToOneIterator<>(myJobQueues.values(), myComparator);

        return new RunnableJobIterator(baseIterator);
    }

    private class RunnableJobIterator extends AbstractIterator<ScheduledJob>
    {
        private final Iterator<ScheduledJob> myBaseIterator;

        RunnableJobIterator(final Iterator<ScheduledJob> baseIterator)
        {
            myBaseIterator = baseIterator;
        }

        @Override
        protected ScheduledJob computeNext()
        {
            while (myBaseIterator.hasNext())
            {
                ScheduledJob job = myBaseIterator.next();

                ScheduledJob.State state = job.getState();
                if (state == ScheduledJob.State.FAILED || state == ScheduledJob.State.FINISHED)
                {
                    LOG.info("{}: {}, descheduling", job, state);
                    job.finishJob();
                    ScheduledJobQueue.this.remove(job);
                }
                else if (state != ScheduledJob.State.PARKED)
                {
                    LOG.debug("Retrieving job: {}, Priority: {}", job, job.getPriority());
                    return job;
                }
            }

            return endOfData();
        }
    }
}
