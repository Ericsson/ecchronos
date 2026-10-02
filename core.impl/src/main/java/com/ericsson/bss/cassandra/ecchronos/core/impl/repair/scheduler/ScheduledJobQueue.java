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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.utils.converter.ManyToOneIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final EnumMap<ScheduledJob.Priority, PriorityQueue<ScheduledJob>> myJobQueues
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
        if (job == null)
        {
            return;
        }
        LOG.debug("Removing job: {}", job);
        myJobQueues.get(job.getPriority()).remove(job);
    }

    private void addJobInternal(final ScheduledJob job)
    {
        LOG.debug("Adding job: {}, Priority: {}, Node: {}", job, job.getPriority(), job.getNodeId());
        PriorityQueue<ScheduledJob> queue = myJobQueues.get(job.getPriority());
        queue.add(job);
        LOG.debug("Size of {} Queue for Node: {} is {}", job.getPriority(), job.getNodeId(), queue.size());
    }

    /**
     * Get the total number of jobs across all priorities.
     *
     * @return the queue size.
     */
    public final int size()
    {
        int size = 0;

        for (PriorityQueue<ScheduledJob> queue : myJobQueues.values())
        {
            size += queue.size();
        }

        return size;
    }

    @Override
    public final Iterator<ScheduledJob> iterator()
    {
        return iterator(job -> true);
    }

    /**
     * Returns an iterator over the runnable jobs, refreshing only the jobs for which {@code shouldRefresh} returns
     * {@code true} before selection.
     * <p>
     * {@link ScheduledJob#refreshState()} can be expensive (for VNODE repair it recomputes O(nodes × tables ×
     * ranges) repair state and performs I/O). Refreshing every job on every scheduler pass burns CPU on jobs that
     * cannot possibly run this pass. Callers should pass a predicate that excludes such jobs (for example those in
     * a backoff window or already failed) using only signals that do not themselves depend on the refreshed state,
     * so that plausibly-eligible jobs are still refreshed and not starved.
     *
     * @param shouldRefresh predicate deciding whether a given job should have its state refreshed this pass.
     * @return an iterator over the currently runnable jobs.
     */
    public final Iterator<ScheduledJob> iterator(final Predicate<ScheduledJob> shouldRefresh)
    {
        List<ScheduledJob> jobsToRefresh;
        synchronized (this)
        {
            jobsToRefresh = myJobQueues.values().stream()
                    .flatMap(Collection::stream)
                    .filter(shouldRefresh)
                    .collect(Collectors.toList());
        }

        // Refresh state outside the lock — refreshState() may perform I/O
        jobsToRefresh.forEach(ScheduledJob::refreshState);

        synchronized (this)
        {
            purgeFinishedJobs();
            List<PriorityQueue<ScheduledJob>> snapshots = myJobQueues.values().stream()
                    .map(PriorityQueue::new)
                    .collect(Collectors.toList());
            Iterator<ScheduledJob> baseIterator = new ManyToOneIterator<>(snapshots, myComparator);
            return new RunnableJobIterator(baseIterator);
        }
    }

    private void purgeFinishedJobs()
    {
        List<ScheduledJob> finishedJobs = new ArrayList<>();
        for (PriorityQueue<ScheduledJob> queue : myJobQueues.values())
        {
            queue.removeIf(job ->
            {
                ScheduledJob.State state = job.getState();
                if (state == ScheduledJob.State.FINISHED || state == ScheduledJob.State.FAILED)
                {
                    LOG.info("{}: {}, descheduling", job, state);
                    finishedJobs.add(job);
                    return true;
                }
                return false;
            });
        }
        finishedJobs.forEach(ScheduledJob::finishJob);
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
                    LOG.debug("Retrieving job: {}, Priority: {} for node {}", job, job.getPriority(), job.getNodeId());
                    return job;
                }
                LOG.debug(" Rejected job: {}, Priority: {}", job, job.getPriority());
            }
            LOG.debug("No jobs available");
            return endOfData();
        }
    }
}


