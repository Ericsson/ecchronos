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

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.utils.logging.ThrottlingLogger;
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
    private static final ThrottlingLogger THROTTLED_LOGGER = new ThrottlingLogger(LOG, 5, TimeUnit.MINUTES);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);

    private static final String NO_RUNNING_JOB = "No job is currently running";

    private final ScheduledJobQueue myQueue = new ScheduledJobQueue(new DefaultJobComparator());
    private final AtomicReference<ScheduledJob> currentExecutingJob = new AtomicReference<>();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final ScheduledFuture<?> myRunFuture;

    private final JobRunTask myRunTask = new JobRunTask();
    private final LockFactory myLockFactory;
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("TaskExecutor-%d").build());

    private ScheduleManagerImpl(final Builder builder)
    {
        myLockFactory = builder.myLockFactory;
        myRunFuture = myExecutor.scheduleWithFixedDelay(myRunTask,
                builder.myRunIntervalInMs,
                builder.myRunIntervalInMs,
                TimeUnit.MILLISECONDS);
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
        THROTTLED_LOGGER.info("Run policy {} added", runPolicy);
        return myRunPolicies.add(runPolicy);
    }

    public boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        THROTTLED_LOGGER.info("Run policy {} removed", runPolicy);
        return myRunPolicies.remove(runPolicy);
    }

    @Override
    public void schedule(final ScheduledJob job)
    {
        myQueue.add(job);
    }

    @Override
    public void deschedule(final ScheduledJob job)
    {
        myQueue.remove(job);
    }

    @Override
    public void close()
    {
        if (myRunFuture != null)
        {
            myRunFuture.cancel(false);
        }

        myExecutor.shutdown();
        myRunPolicies.clear();
    }

    /**
     * Made available for testing.
     */
    @VisibleForTesting
    public void run()
    {
        myRunTask.run();
    }

    /**
     * Made available for testing.
     *
     * @return int Queue size.
     */
    @VisibleForTesting
    public int getQueueSize()
    {
        return myQueue.size();
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
    private class JobRunTask implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                tryRunNext();
            }
            catch (Exception e)
            {
                LOG.error("Unexpected exception while running job", e);
            }
        }

        private void tryRunNext()
        {
            for (ScheduledJob next : myQueue)
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

        private boolean tryRunTasks(final ScheduledJob next)
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

        private boolean tryRunTask(final ScheduledJob job, final ScheduledTask task)
        {
            LOG.debug("Trying to acquire lock for {}", task);
            try (LockFactory.DistributedLock lock = task.getLock(myLockFactory))
            {
                boolean successful = runTask(task);
                job.postExecute(successful, task);
                return true;
            }
            catch (LockException e)
            {
                if (e.getCause() != null)
                {
                    LOG.warn("Unable to get schedule lock on task {}", task, e);
                }
                return false;
            }
        }

        private boolean runTask(final ScheduledTask task)
        {
            try
            {
                LOG.info("Running task: {}", task);
                return task.execute();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to run task: {}", task, e);
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
        private LockFactory myLockFactory;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;

        public final Builder withLockFactory(final LockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        public final Builder withRunInterval(final long runInterval, final TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }


        public final ScheduleManagerImpl build()
        {
            return new ScheduleManagerImpl(this);
        }
    }
}
