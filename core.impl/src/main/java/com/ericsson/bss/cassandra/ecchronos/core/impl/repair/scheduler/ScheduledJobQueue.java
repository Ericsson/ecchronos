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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Dynamic priority queue for scheduled jobs.
 * <p>
 * Jobs are held in a single list and the runnable-job ordering is produced on each {@link #iterator(Predicate)} call
 * by building exactly one sorted snapshot, keyed on a per-pass-frozen {@link ScheduledJob#getRealPriority()} value.
 * <p>
 * Earlier revisions kept an {@code EnumMap<Priority, PriorityQueue>} and copied every per-priority heap on each pass
 * before re-sorting; the heaps were vestigial because {@link ScheduledJob#getRealPriority()} is wall-clock dependent,
 * so the real ordering was always produced by the fresh per-pass sort rather than by the heap invariant. Flattening
 * to a single list removes that redundant per-pass snapshot copy (roughly halving per-pass allocation) while
 * preserving selection order. See issue #1903 (comparator stability landed earlier as #1904).
 */
public class ScheduledJobQueue implements Iterable<ScheduledJob>
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduledJobQueue.class);

    private final List<ScheduledJob> myJobs = new ArrayList<>();

    /**
     * Construct a new job queue.
     *
     * @param comparator
     *            Retained for API/backwards compatibility with callers (for example
     *            {@link DefaultJobComparator}). The effective ordering is produced per pass from a frozen
     *            {@link ScheduledJob#getRealPriority()} key (see {@link #frozenPriorityComparator(Collection)}),
     *            which matches the configured comparator's ordering while being contract-safe during sorting.
     */
    public ScheduledJobQueue(final Comparator<ScheduledJob> comparator)
    {
        // The comparator is intentionally not stored: ordering is produced from a per-pass-frozen key (#1903/#1904).
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
        // Identity removal: jobs are compared by reference, matching the previous PriorityQueue.remove semantics for
        // these (equals-less) job instances.
        myJobs.removeIf(queued -> queued == job); // NOPMD CompareObjectsWithEquals - intentional identity removal
    }

    private void addJobInternal(final ScheduledJob job)
    {
        LOG.debug("Adding job: {}, Priority: {}, Node: {}", job, job.getPriority(), job.getNodeId());
        myJobs.add(job);
        LOG.debug("Size of queue for Node: {} is {}", job.getNodeId(), myJobs.size());
    }

    @VisibleForTesting
    final synchronized int size()
    {
        return myJobs.size();
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
            jobsToRefresh = myJobs.stream()
                    .filter(shouldRefresh)
                    .collect(Collectors.toList());
        }

        // Refresh state outside the lock — refreshState() may perform I/O.
        jobsToRefresh.forEach(ScheduledJob::refreshState);

        synchronized (this)
        {
            purgeFinishedJobs();
            // Build exactly one sorted snapshot for this pass. Iterating this copy (rather than the live list) keeps
            // mutation-during-iteration safe: RunnableJobIterator and concurrent add/remove mutate myJobs without
            // disturbing the iteration.
            List<ScheduledJob> snapshot = new ArrayList<>(myJobs);
            snapshot.sort(frozenPriorityComparator(snapshot));
            return new RunnableJobIterator(snapshot.iterator());
        }
    }

    /**
     * Build a comparator over a per-pass-stable priority key: each job's {@link ScheduledJob#getRealPriority()} is
     * read exactly once here and frozen, so no wall-clock reads occur during the subsequent sort. The tie-break on
     * configured {@link ScheduledJob#getPriority() priority} is time-independent and read live, matching the
     * ordering of {@link DefaultJobComparator}. See issues #1903 / #1904.
     *
     * @param jobs the jobs to be sorted this pass.
     * @return a comparator backed by the frozen real-priority values.
     */
    private static Comparator<ScheduledJob> frozenPriorityComparator(final Collection<ScheduledJob> jobs)
    {
        Map<ScheduledJob, Integer> frozenRealPriority = new IdentityHashMap<>();
        for (ScheduledJob job : jobs)
        {
            frozenRealPriority.computeIfAbsent(job, ScheduledJob::getRealPriority);
        }
        return (j1, j2) ->
        {
            int result = Integer.compare(frozenRealPriority.get(j2), frozenRealPriority.get(j1));
            if (result == 0)
            {
                result = Integer.compare(j2.getPriority().getValue(), j1.getPriority().getValue());
            }
            return result;
        };
    }

    private void purgeFinishedJobs()
    {
        List<ScheduledJob> finishedJobs = new ArrayList<>();
        myJobs.removeIf(job ->
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
