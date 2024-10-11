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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleManager handles the run scheduler and update scheduler.
 */
public final class ScheduleManagerImpl implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerImpl.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);

    private static final String NO_RUNNING_JOB = "No job is currently running";

    private final Map<UUID, ScheduledJobQueue> myQueue = new ConcurrentHashMap<>();
    private final Collection<UUID> myNodeIDList;
    private final AtomicReference<ScheduledJob> currentExecutingJob = new AtomicReference<>();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final Map<UUID, ScheduledFuture<?>> myRunFuture = new ConcurrentHashMap<>();
    private final Map<UUID, JobRunTask> myRunTasks = new ConcurrentHashMap<>();

    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("TaskExecutor-%d").build());

    private ScheduleManagerImpl(final Builder builder)
    {
        myNodeIDList = builder.myNodeIDList;
        createScheduleFutureForNodeIDList(builder);
    }

    private void createScheduleFutureForNodeIDList(final Builder builder)
    {
        for (UUID nodeID : myNodeIDList)
        {
            JobRunTask myRunTask = new JobRunTask(nodeID);
            ScheduledFuture<?> scheduledFuture = myExecutor.scheduleWithFixedDelay(myRunTask,
                    builder.myRunIntervalInMs,
                    builder.myRunIntervalInMs,
                    TimeUnit.MILLISECONDS);
            myRunTasks.put(nodeID, myRunTask);
            myRunFuture.put(nodeID, scheduledFuture);
        }
    }

    @Override
    public String getCurrentJobStatus()
    {
        ScheduledJob job = currentExecutingJob.get();
        if (job != null)
        {
            String jobId = job.getId().toString();
            return "Job ID: " + jobId + ", Status: Running";
        }
        else
        {
            return ScheduleManagerImpl.NO_RUNNING_JOB;
        }
    }
    public boolean addRunPolicy(final RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} added", runPolicy);
        return myRunPolicies.add(runPolicy);
    }

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
     */
    @VisibleForTesting
    public void run(final UUID nodeID)
    {
        myRunTasks.get(nodeID).run();
    }

    /**
     * Made available for testing.
     *
     * @return int Queue size.
     */
    @VisibleForTesting
    public int getQueueSize(final UUID nodeID)
    {
        return myQueue.get(nodeID).size();
    }

    private Long validateJob(final ScheduledJob job)
    {
        for (RunPolicy runPolicy : myRunPolicies)
        {
            long nextRun = runPolicy.validate(job);
            if (nextRun != -1)
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

        private JobRunTask(final UUID currentNodeID)
        {
            nodeID = currentNodeID;
        }

        @Override
        public void run()
        {
            try
            {
                tryRunNext();
            }
            catch (Exception e)
            {
                LOG.error("Unexpected exception while running job in node {}", nodeID, e);
            }
        }

        private void tryRunNext()
        {
            for (ScheduledJob next : myQueue.get(nodeID))
            {
                if (validate(next))
                {
                    currentExecutingJob.set(next);
                    if (tryRunTasks(next))
                    {
                        break;
                    }
                }
            }
            currentExecutingJob.set(null);
        }

        private boolean validate(final ScheduledJob job)
        {
            LOG.trace("Validating job {}", job);
            long nextRun = validateJob(job);

            if (nextRun != -1)
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

            for (ScheduledTask task : next)
            {
                if (!validate(next))
                {
                    LOG.info("Job {} was stopped, will continue later", next);
                    break;
                }
                hasRun |= tryRunTask(next, task);
            }

            return hasRun;
        }

        private boolean tryRunTask(
                final ScheduledJob job,
                final ScheduledTask task)
        {
            LOG.debug("Trying to run task {} in node {}", task, nodeID);
            // need to implement lock mechanism
            try
            {
                boolean successful = runTask(task);
                job.postExecute(successful);
                return true;
            }
            catch (Exception e)
            {
                if (e.getCause() != null)
                {
                    LOG.warn("Unable to get schedule lock on task {} in node {}", task, nodeID, e);
                }
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Collection<UUID> myNodeIDList;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;

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

        public final ScheduleManagerImpl build()
        {
            return new ScheduleManagerImpl(this);
        }
    }
}

