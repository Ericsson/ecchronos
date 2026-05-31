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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.RuntimeMBeanException;

/**
 * ScheduleManager handles the run scheduler and update scheduler.
 */
public final class ScheduleManagerImpl implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerImpl.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);
    public static final int DEFAULT_KEEP_ALIVE_TIME = 60;
    public static final int DEFAULT_TIMEOUT = 5;

    private final Map<UUID, ScheduledJobQueue> myQueue = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, ScheduledJob> currentExecutingJobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ScheduledJob, Long> myContentionBackoff = new ConcurrentHashMap<>();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final Map<UUID, ScheduledFuture<?>> myRunFuture = new ConcurrentHashMap<>();
    private final Map<UUID, JobRunTask> myRunTasks = new ConcurrentHashMap<>();
    private final CASLockFactory myLockFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private final ScheduledThreadPoolExecutor myExecutor;
    private final long myRunIntervalInMs;
    private final long mySessionWindowInMs;
    private final long myCooldownInMs;

    private ScheduleManagerImpl(final Builder builder)
    {
        Collection<UUID> nodeIDList = builder.myNodeIDList;
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        myExecutor  = new ScheduledThreadPoolExecutor(
                nodeIDList.size(), new ThreadFactoryBuilder().setNameFormat("TaskExecutor-%d").build());
        myExecutor.setKeepAliveTime(DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS);
        myExecutor.allowCoreThreadTimeOut(true);
        myLockFactory = builder.myLockFactory;
        myRunIntervalInMs = builder.myRunIntervalInMs;
        mySessionWindowInMs = builder.mySessionWindowInMs;
        myCooldownInMs = builder.myCooldownInMs;
    }

    /**
     * Create a ScheduledFuture for each of the nodes in the nodeIDList.
     * @param nodeIDList
     */
    @Override
    public void createScheduleFutureForNodeIDList(final Collection<UUID> nodeIDList)
    {
        LOG.debug("Total nodes found: {}", nodeIDList.size());
        for (UUID nodeID : nodeIDList)
        {
            createScheduleFutureForNode(nodeID);
        }
    }

    /**
     * Create a ScheduledFuture for  the nodeID.
     * @param nodeID
     */
    @Override
    public void createScheduleFutureForNode(final UUID nodeID)
    {
        myRunTasks.computeIfAbsent(nodeID, id ->
        {
            JobRunTask runTask = new JobRunTask(id);
            int requiredSize = myRunTasks.size() + 1;
            if (myExecutor.getCorePoolSize() < requiredSize)
            {
                myExecutor.setCorePoolSize(requiredSize);
            }
            ScheduledFuture<?> scheduledFuture = myExecutor.schedule(runTask,
                    myRunIntervalInMs,
                    TimeUnit.MILLISECONDS);
            myRunFuture.put(id, scheduledFuture);
            LOG.debug("JobRunTask created for node {}", id);
            return runTask;
        });
    }
    @Override
    public String getCurrentJobStatus()
    {
        Set<Map.Entry<UUID, ScheduledJob>> jobs = currentExecutingJobs.entrySet();
        if (!jobs.isEmpty())
        {
            String result = "";
            for (Map.Entry<UUID, ScheduledJob> job : jobs)
            {
                result += job.getValue().getJobId() + " on node " + job.getKey() + ", ";
            }
            return result;
        }
        else
        {
            return "";
        }
    }

    /**
     * Adds a run policy to the collection of run policies.
     *
     * @param runPolicy the {@link RunPolicy} to be added. Must not be {@code null}.
     * @return {@code true} if the run policy was added successfully; {@code false} if it was already present.
     */
    public boolean addRunPolicy(final RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} added", runPolicy);
        return myRunPolicies.add(runPolicy);
    }

    /**
     * Removes a run policy from the collection of run policies.
     *
     * @param runPolicy the {@link RunPolicy} to be removed. Must not be {@code null}.
     * @return {@code true} if the run policy was successfully removed; {@code false} if it was not present.
     */

    public boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} removed", runPolicy);
        return myRunPolicies.remove(runPolicy);
    }

    @Override
    public void schedule(
            final UUID nodeID,
            final ScheduledJob job)
    {
        myQueue.computeIfAbsent(nodeID, k -> new ScheduledJobQueue(new DefaultJobComparator())).add(job);
    }

    @Override
    public void deschedule(final UUID nodeID, final ScheduledJob job)
    {
        ScheduledJobQueue queue = myQueue.get(nodeID);
        if (queue != null)
        {
            queue.remove(job);
        }
        myContentionBackoff.remove(job);
    }

    @Override
    public void close()
    {
        for (ScheduledFuture<?> future : myRunFuture.values())
        {
            future.cancel(false);
        }
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(DEFAULT_TIMEOUT, TimeUnit.MINUTES))
            {
                LOG.warn("Executor did not terminate within timeout, forcing shutdown");
                myExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            LOG.warn("Interrupted while waiting for executor termination", e);
            myExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        myRunFuture.clear();
        myRunTasks.clear();
        myQueue.clear();
        currentExecutingJobs.clear();
        myContentionBackoff.clear();
        myRunPolicies.clear();
    }

    @Override
    public void removeScheduleFutureForNode(final UUID nodeID)
    {
        ScheduledFuture<?> future = myRunFuture.remove(nodeID);
        if (future != null)
        {
            future.cancel(false);
        }
        myRunTasks.remove(nodeID);
        myQueue.remove(nodeID);
        myExecutor.setCorePoolSize(Math.max(1, myRunTasks.size()));
        LOG.info("Removed schedule future and queue for node {}", nodeID);
    }

    /**
     * Made available for testing.
     *
     * @param nodeID the node id to run jobs.
     */
    @VisibleForTesting
    public void run(final UUID nodeID)
    {
        myRunTasks.get(nodeID).run();
    }

    /**
     * Made available for testing.
     *
     * @param nodeID the node id to get queue size.
     * @return int Queue size.
     */
    @VisibleForTesting
    public int getQueueSize(final UUID nodeID)
    {
        return myQueue.get(nodeID).size();
    }

    private Long validateJob(final ScheduledJob job, final Node node)
    {
        for (RunPolicy runPolicy : myRunPolicies)
        {
            long nextRun = runPolicy.validate(job, node);
            if (nextRun != -1L)
            {
                LOG.debug("Job {} rejected for {} ms by {}", job, nextRun, runPolicy);
                return nextRun;
            }
        }

        return -1L;
    }


    /**
     * Internal run task that is scheduled by the {@link ScheduleManagerImpl}.
     * <p>
     * Retrieves a job from the queue and tries to run it provided that it's possible to get the required locks.
     */
    private final class JobRunTask implements Runnable
    {
        private final UUID nodeID;
        private final Node myNode;
        private volatile long myCooldownUntil = 0;

        private JobRunTask(final UUID currentNodeID)
        {
            nodeID = currentNodeID;
            myNode = myNativeConnectionProvider.getNodes().get(nodeID);
        }

        @Override
        public void run()
        {
            boolean hadWork = false;
            try
            {
                LOG.debug("In JobRunTask.run for Node {}", nodeID);
                if (System.currentTimeMillis() < myCooldownUntil)
                {
                    LOG.debug("Node {} in cooldown until {}", nodeID, myCooldownUntil);
                    return;
                }
                if (myQueue.get(nodeID) != null)
                {
                    hadWork = tryRunNext();
                }
                else
                {
                    LOG.info("There is no ScheduledJob for this node {} to run ", nodeID);
                }
            }
            catch (Exception e)
            {
                LOG.error("Exception while running job in node {}", nodeID, e);
            }
            finally
            {
                reschedule(hadWork);
            }
        }

        private void reschedule(final boolean hadWork)
        {
            long delay;
            if (System.currentTimeMillis() < myCooldownUntil)
            {
                delay = myCooldownUntil - System.currentTimeMillis();
            }
            else if (hadWork)
            {
                delay = TimeUnit.SECONDS.toMillis(1);
            }
            else
            {
                delay = myRunIntervalInMs;
            }
            try
            {
                ScheduledFuture<?> future = myExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
                myRunFuture.put(nodeID, future);
            }
            catch (java.util.concurrent.RejectedExecutionException e)
            {
                LOG.debug("Scheduler shutting down, not rescheduling for node {}", nodeID);
            }
        }

        private boolean tryRunNext()
        {
            LOG.debug("Looking for Job for Node {}", nodeID);
            long tickStart = System.currentTimeMillis();
            boolean hadWork = false;
            for (ScheduledJob next : myQueue.get(nodeID))
            {
                if (System.currentTimeMillis() - tickStart > myRunIntervalInMs)
                {
                    break;
                }
                Long backoffUntil = myContentionBackoff.get(next);
                if (backoffUntil != null)
                {
                    if (System.currentTimeMillis() < backoffUntil)
                    {
                        continue;
                    }
                    myContentionBackoff.remove(next);
                }
                if (validate(next))
                {
                    currentExecutingJobs.put(nodeID, next);
                    if (tryRunBatchedSession(next, tickStart))
                    {
                        hadWork = true;
                        break;
                    }
                }
            }
            currentExecutingJobs.remove(nodeID);
            return hadWork;
        }

        private boolean validate(final ScheduledJob job)
        {
            LOG.trace("Validating job {}", job);
            long nextRun = validateJob(job, myNode);

            if (nextRun != -1L)
            {
                job.setRunnableIn(nextRun);
                return false;
            }

            return true;
        }

        private boolean tryRunBatchedSession(final ScheduledJob firstJob, final long tickStart)
        {
            // Try to acquire lock using the first task of the first job
            Iterator<ScheduledTask> taskIterator = firstJob.iterator();
            if (!taskIterator.hasNext())
            {
                return false;
            }

            ScheduledTask firstTask = taskIterator.next();
            LockFactory.DistributedLock sessionLock;
            try
            {
                sessionLock = firstTask.getLock(myLockFactory, nodeID);
            }
            catch (RuntimeMBeanException e)
            {
                handleLockFailure(firstJob, e);
                return false;
            }
            catch (Exception e)
            {
                handleLockFailure(firstJob, e);
                return false;
            }

            // Lock acquired - run batched session
            long sessionStart = System.currentTimeMillis();
            int tasksExecuted = 0;
            try
            {
                LOG.info("Batched session started for node {}, window={}ms", nodeID, mySessionWindowInMs);

                // Execute the first task (lock already acquired for it)
                if (runTask(firstTask, 1))
                {
                    firstJob.postExecute(true, firstTask);
                    tasksExecuted++;
                }
                else
                {
                    firstJob.postExecute(false, firstTask);
                }

                // Continue with remaining tasks of the first job
                int index = 1;
                while (taskIterator.hasNext() && withinSessionWindow(sessionStart))
                {
                    index++;
                    ScheduledTask task = taskIterator.next();
                    boolean successful = runTask(task, index);
                    firstJob.postExecute(successful, task);
                    if (successful)
                    {
                        tasksExecuted++;
                    }
                }

                if (tasksExecuted > 0)
                {
                    firstJob.refreshState();
                }

                // Continue with other jobs while within session window
                if (withinSessionWindow(sessionStart))
                {
                    tasksExecuted += runRemainingJobsInSession(sessionStart, firstJob);
                }
            }
            finally
            {
                sessionLock.close();
                long elapsed = System.currentTimeMillis() - sessionStart;
                LOG.info("Batched session ended for node {}, executed {} tasks in {}ms",
                        nodeID, tasksExecuted, elapsed);

                if (tasksExecuted > 0 && myCooldownInMs > 0)
                {
                    myCooldownUntil = System.currentTimeMillis() + myCooldownInMs;
                    LOG.debug("Node {} entering cooldown for {}ms", nodeID, myCooldownInMs);
                }
            }
            return true;
        }

        private int runRemainingJobsInSession(final long sessionStart, final ScheduledJob excludeJob)
        {
            int tasksExecuted = 0;
            for (ScheduledJob job : myQueue.get(nodeID))
            {
                if (!withinSessionWindow(sessionStart))
                {
                    break;
                }
                if (job.equals(excludeJob))
                {
                    continue;
                }
                Long backoffUntil = myContentionBackoff.get(job);
                if (backoffUntil != null && System.currentTimeMillis() < backoffUntil)
                {
                    continue;
                }
                if (!validate(job))
                {
                    continue;
                }
                currentExecutingJobs.put(nodeID, job);
                int index = 0;
                boolean jobHadWork = false;
                for (ScheduledTask task : job)
                {
                    if (!withinSessionWindow(sessionStart))
                    {
                        break;
                    }
                    index++;
                    boolean successful = runTask(task, index);
                    job.postExecute(successful, task);
                    if (successful)
                    {
                        tasksExecuted++;
                        jobHadWork = true;
                    }
                }
                if (jobHadWork)
                {
                    job.refreshState();
                }
            }
            return tasksExecuted;
        }

        private boolean withinSessionWindow(final long sessionStart)
        {
            return (System.currentTimeMillis() - sessionStart) < mySessionWindowInMs;
        }

        private void handleLockFailure(final ScheduledJob job, final Exception e)
        {
            if (e instanceof RuntimeMBeanException)
            {
                RuntimeMBeanException rme = (RuntimeMBeanException) e;
                if (rme.getCause() instanceof IllegalStateException
                        && rme.getCause().getMessage() != null
                        && rme.getCause().getMessage().contains("More than one key found"))
                {
                    LOG.debug("Unable to get schedule lock on job {} in node {}, probably Jolokia 2.3.0 or older",
                            job, nodeID, e);
                }
                else
                {
                    LOG.warn("Unable to get schedule lock on job {} in node {}", job, nodeID, e);
                }
            }
            else if (e.getCause() != null)
            {
                LOG.warn("Unable to get schedule lock on job {} in node {}", job, nodeID, e);
            }
            else
            {
                LOG.debug("Lock contention for job {} in node {}", job, nodeID, e);
            }
            long backoff = ThreadLocalRandom.current().nextLong(
                    myRunIntervalInMs / 2, myRunIntervalInMs);
            myContentionBackoff.put(job, System.currentTimeMillis() + backoff);
        }

        private boolean runTask(
                final ScheduledTask task,
                final int index)
        {
            try
            {
                LOG.debug("Running task: {} ({}), for node {}", task, index, nodeID);
                return task.execute(nodeID);
            }
            catch (Exception e)
            {
                LOG.warn("Unable to run task: {} in node: {}", task, nodeID, e);
            }

            return false;
        }
    }

    /**
     * Create an instance of Builder to construct ScheduleManagerImpl.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder class to construct ScheduleManagerImpl.
     */
    public static class Builder
    {
        private static final long DEFAULT_SESSION_WINDOW_MS = TimeUnit.MINUTES.toMillis(5);
        private Collection<UUID> myNodeIDList;
        private CASLockFactory myLockFactory;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;
        private long mySessionWindowInMs = DEFAULT_SESSION_WINDOW_MS;
        private long myCooldownInMs = 0;
        private DistributedNativeConnectionProvider myNativeConnectionProvider;

        /**
         * Build SchedulerManager with run interval.
         *
         * @param runInterval the interval to run a repair task
         * @param timeUnit the TimeUnit to specify the interval
         * @return Builder with run interval
         */
        public final Builder withRunInterval(final long runInterval, final TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }

        /**
         * Build SchedulerManager with session window.
         *
         * @param sessionWindow the time window for batched lock sessions
         * @param timeUnit the TimeUnit to specify the window
         * @return Builder with session window
         */
        public final Builder withSessionWindow(final long sessionWindow, final TimeUnit timeUnit)
        {
            mySessionWindowInMs = timeUnit.toMillis(sessionWindow);
            return this;
        }

        /**
         * Build SchedulerManager with cooldown.
         *
         * @param cooldown the cooldown after a batched session
         * @param timeUnit the TimeUnit to specify the cooldown
         * @return Builder with cooldown
         */
        public final Builder withCooldown(final long cooldown, final TimeUnit timeUnit)
        {
            myCooldownInMs = timeUnit.toMillis(cooldown);
            return this;
        }

        /**
         * Build SchedulerManager with run interval.
         *
         * @param nodeIDList the interval to run a repair task
         * @return Builder with nodes list
         */
        public Builder withNodeIDList(final Collection<UUID> nodeIDList)
        {
            myNodeIDList = nodeIDList;
            return this;
        }

        /**
         * Sets the {@link CASLockFactory}.
         *
         * @param lockFactory The {@link CASLockFactory} to be used.
         *                    Must not be {@code null}.
         * @return The current {@code Builder} instance, allowing for method chaining.
         */
        public final Builder withLockFactory(final CASLockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        /**
         * Build SchedulerManager with native connection provider.
         *
         * @param nativeConnectionProvider the native connection provider
         * @return Builder with native connection provider
         */
        public Builder withNativeConnectionProvider(final DistributedNativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnectionProvider = nativeConnectionProvider;
            return this;
        }

        /**
         * Build SchedulerManager with the provided configuration.
         *
         * @return ScheduleManagerImpl with provided configuration.
         */
        public final ScheduleManagerImpl build()
        {
            return new ScheduleManagerImpl(this);
        }
    }
}

