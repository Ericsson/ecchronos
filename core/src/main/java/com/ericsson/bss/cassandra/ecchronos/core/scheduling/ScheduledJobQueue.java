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

import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.utils.ManyToOneIterator;
import com.google.common.annotations.VisibleForTesting;

/**
 * Dynamic priority queue for scheduled jobs.
 * <p>
 * This queue is divided in several smaller queues, one for each {@link ScheduledJob.Priority priority type} and are then retrieved using a
 * {@link ManyToOneIterator}.
 */
public class ScheduledJobQueue implements Iterable<ScheduledJob>
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJobQueue.class);

    private final Comparator<ScheduledJob> myComparator;

    private final EnumMap<ScheduledJob.Priority, PriorityQueue<ScheduledJob>> myJobQueues = new EnumMap<>(ScheduledJob.Priority.class);

    /**
     * Construct a new job queue that prioritizes the jobs based on the provided comparator.
     *
     * @param comparator
     *            The comparator used to determine the job with the highest priority.
     */
    public ScheduledJobQueue(Comparator<ScheduledJob> comparator)
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
    public synchronized void add(ScheduledJob job)
    {
        addJobInternal(job);
    }

    /**
     * Add a collection of jobs to the queue at once.
     *
     * @param jobs
     *            The collection of jobs.
     */
    public synchronized void addAll(Collection<? extends ScheduledJob> jobs)
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
    public synchronized void remove(ScheduledJob job)
    {
        LOG.debug("Removing job: {}", job);
        myJobQueues.get(job.getPriority()).remove(job);
    }

    private void addJobInternal(ScheduledJob job)
    {
        LOG.debug("Adding job: {}, Priority: {}", job, job.getPriority());
        myJobQueues.get(job.getPriority()).add(job);
    }

    @VisibleForTesting
    int size()
    {
        int size = 0;

        for (PriorityQueue<ScheduledJob> queue : myJobQueues.values())
        {
            size += queue.size();
        }

        return size;
    }

    @Override
    public synchronized Iterator<ScheduledJob> iterator()
    {
        Iterator<ScheduledJob> baseIterator = new ManyToOneIterator<>(myJobQueues.values(), myComparator);

        return new RunnableJobIterator(baseIterator);
    }

    private class RunnableJobIterator extends AbstractIterator<ScheduledJob>
    {
        private final Iterator<ScheduledJob> myBaseIterator;

        public RunnableJobIterator(Iterator<ScheduledJob> baseIterator)
        {
            myBaseIterator = baseIterator;
        }

        @Override
        protected ScheduledJob computeNext()
        {
            while(myBaseIterator.hasNext())
            {
                ScheduledJob job = myBaseIterator.next();

                if (job.getState() == ScheduledJob.State.FAILED)
                {
                    LOG.error("{} failed, descheduling", job);
                    ScheduledJobQueue.this.remove(job);
                }
                else if (job.getState() == ScheduledJob.State.FINISHED)
                {
                    LOG.debug("{} completed, descheduling", job);
                    ScheduledJobQueue.this.remove(job);
                }
                else if (job.getState() != ScheduledJob.State.PARKED)
                {
                    LOG.debug("Retrieving job: {}, Priority: {}", job, job.getPriority());
                    return job;
                }
            }

            return endOfData();
        }
    }
}
