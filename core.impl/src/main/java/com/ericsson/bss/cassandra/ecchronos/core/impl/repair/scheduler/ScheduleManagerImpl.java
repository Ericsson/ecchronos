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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    private final Map<UUID, ScheduledJobQueue> myQueue = new ConcurrentHashMap<>();
    private final Collection<UUID> myNodeIDList;
    private final Map<UUID, ScheduledJob> currentExecutingJobs = new ConcurrentHashMap<>();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final Map<UUID, ScheduledFuture<?>> myRunFuture = new ConcurrentHashMap<>();
    private final Map<UUID, JobRunTask> myRunTasks = new ConcurrentHashMap<>();
    private final CASLockFactory myLockFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private final ScheduledThreadPoolExecutor myExecutor;
    private final long myRunIntervalInMs;

    private ScheduleManagerImpl(final Builder builder)
    {
        myNodeIDList = builder.myNodeIDList;
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        myExecutor  = new ScheduledThreadPoolExecutor(
                myNodeIDList.size(), new ThreadFactoryBuilder().setNameFormat("TaskExecutor-%d").build());
        myLockFactory = builder.myLockFactory;
        myRunIntervalInMs = builder.myRunIntervalInMs;
    }

    /**
     * Create a ScheduledFuture for each of the nodes in the nodeIDList.
     * @param nodeIDList
     */
    @Override
    public void createScheduleFutureForNodeIDList(final Collection<UUID> nodeIDList)
    {
        myExecutor.setCorePoolSize(nodeIDList.size());
        LOG.debug("Total nodes found: {}", nodeIDList.size());
        for (UUID nodeID : nodeIDList)
        {
            if (myRunTasks.get(nodeID) == null)
            {
                JobRunTask myRunTask = new JobRunTask(nodeID);
                ScheduledFuture<?> scheduledFuture = myExecutor.scheduleWithFixedDelay(myRunTask,
                        myRunIntervalInMs,
                        myRunIntervalInMs,
                        TimeUnit.MILLISECONDS);
                myRunTasks.put(nodeID, myRunTask);
                myRunFuture.put(nodeID, scheduledFuture);
                LOG.debug("JobRunTask created for node {}", nodeID);
            }
        }
    }

    /**
     * Create a ScheduledFuture for  the nodeID.
     * @param nodeID
     */
    @Override
    public void createScheduleFutureForNode(final UUID nodeID)
    {
        if (myRunTasks.get(nodeID) == null)
        {
            JobRunTask myRunTask = new JobRunTask(nodeID);
            myExecutor.setCorePoolSize(myRunTasks.size());

            ScheduledFuture<?> scheduledFuture = myExecutor.scheduleWithFixedDelay(myRunTask,
                    myRunIntervalInMs,
                    myRunIntervalInMs,
                    TimeUnit.MILLISECONDS);
            myRunTasks.put(nodeID, myRunTask);
            myRunFuture.put(nodeID, scheduledFuture);
            myExecutor.setCorePoolSize(myRunTasks.size());
            LOG.debug("JobRunTask created for new node {}", nodeID);
        }
        else
        {
            LOG.debug("JobRunTask already exists for new node {}", nodeID);
        }
    }
    @Override
    public String getCurrentJobStatus()
    {
        return currentExecutingJobs.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().getJobId())
                .collect(Collectors.joining("\n"));
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
        ScheduledJobQueue queue = myQueue.get(nodeID);
        if (queue == null)
        {
            myQueue.put(nodeID, new ScheduledJobQueue(new DefaultJobComparator()));
        }
        myQueue.get(nodeID).add(job);
    }

    @Override
    public void deschedule(final UUID nodeID, final ScheduledJob job)
    {
        myQueue.get(nodeID).remove(job);
    }

    @Override
    public void close()
    {
        for (ScheduledFuture<?> future : myRunFuture.values())
        {
            future.cancel(false);
        }
        myExecutor.shutdown();
        myRunPolicies.clear();
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

        private JobRunTask(final UUID currentNodeID)
        {
            nodeID = currentNodeID;
            myNode = myNativeConnectionProvider.getNodes().get(nodeID);
        }

        @Override
        public void run()
        {
            try
            {
                LOG.debug("In JobRunTask.run for Node {}", nodeID);
                if (myQueue.get(nodeID) != null)
                {
                    tryRunNext();
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
        }

        private void tryRunNext()
        {
            long cycleStart = System.currentTimeMillis();
            int jobsEvaluated = 0;
            int jobsValidated = 0;
            int jobsRejectedByPolicy = 0;
            boolean jobRan = false;
            LOG.info("[DIAG] Node {} - Starting scheduler cycle, queue size: {}",
                    nodeID, myQueue.get(nodeID).size());
            for (ScheduledJob next : myQueue.get(nodeID))
            {
                jobsEvaluated++;
                if (validate(next))
                {
                    jobsValidated++;
                    currentExecutingJobs.put(nodeID, next);
                    if (tryRunTasks(next))
                    {
                        jobRan = true;
                        break;
                    }
                }
                else
                {
                    jobsRejectedByPolicy++;
                }
            }
            long cycleDuration = System.currentTimeMillis() - cycleStart;
            LOG.info("[DIAG] Node {} - Cycle completed in {}ms. Jobs evaluated: {}, validated: {},"
                    + " rejected by policy: {}, ran: {}",
                    nodeID, cycleDuration, jobsEvaluated, jobsValidated, jobsRejectedByPolicy, jobRan);
            currentExecutingJobs.remove(nodeID);
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

        private boolean tryRunTasks(
                final ScheduledJob next)
        {
            boolean hasRun = false;
            int taskCount = 0;
            int taskSuccess = 0;
            int taskLockFailed = 0;
            long jobStart = System.currentTimeMillis();
            LOG.info("[DIAG] Node {} - Starting tasks for job {} (priority: {})",
                    nodeID, next, next.getRealPriority());

            for (ScheduledTask task : next)
            {
                taskCount++;
                if (!validate(next))
                {
                    LOG.info("[DIAG] Node {} - Job {} stopped by policy after {} tasks",
                            nodeID, next, taskCount);
                    break;
                }
                boolean taskRan = tryRunTask(next, task);
                if (taskRan)
                {
                    taskSuccess++;
                }
                else
                {
                    taskLockFailed++;
                }
                hasRun |= taskRan;
            }
            long jobDuration = System.currentTimeMillis() - jobStart;
            LOG.info("[DIAG] Node {} - Job {} finished in {}ms. Tasks: {}, success: {}, lock failed: {}",
                    nodeID, next, jobDuration, taskCount, taskSuccess, taskLockFailed);

            return hasRun;
        }

        private boolean tryRunTask(
                final ScheduledJob job,
                final ScheduledTask task)
        {
            long lockStart = System.currentTimeMillis();
            try (LockFactory.DistributedLock lock = task.getLock(myLockFactory, nodeID))
            {
                long lockTime = System.currentTimeMillis() - lockStart;
                LOG.info("[DIAG] Node {} - Lock acquired for {} in {}ms", nodeID, task, lockTime);
                long taskStart = System.currentTimeMillis();
                boolean successful = runTask(task);
                long taskDuration = System.currentTimeMillis() - taskStart;
                LOG.info("[DIAG] Node {} - Task {} executed in {}ms, successful: {}",
                        nodeID, task, taskDuration, successful);
                job.postExecute(successful, task);
                return true;
            }
            catch (RuntimeMBeanException e)
            {
                long lockTime = System.currentTimeMillis() - lockStart;
                if (e.getCause() instanceof IllegalStateException
                        && e.getCause().getMessage() != null
                        && e.getCause().getMessage().contains("More than one key found"))
                {
                    LOG.info("[DIAG] Node {} - Lock failed for {} in {}ms (Jolokia compat issue)",
                            nodeID, task, lockTime);
                }
                else
                {
                    LOG.warn("[DIAG] Node {} - Lock failed for {} in {}ms: {}",
                            nodeID, task, lockTime, e.getMessage());
                }
                return false;
            }
            catch (Exception e)
            {
                long lockTime = System.currentTimeMillis() - lockStart;
                LOG.warn("[DIAG] Node {} - Lock failed for {} in {}ms: {}",
                        nodeID, task, lockTime, e.getMessage());
                return false;
            }
        }

        private boolean runTask(
                final ScheduledTask task)
        {
            try
            {
                LOG.info("Running task: {}, for node {}", task, nodeID);
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
        private Collection<UUID> myNodeIDList;
        private CASLockFactory myLockFactory;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;
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

