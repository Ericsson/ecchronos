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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleManager handles the run scheduler and update scheduler.
 */
public class ScheduleManagerImpl implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerImpl.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);

    private final ScheduledJobQueue myQueue = new ScheduledJobQueue(new DefaultJobComparator());
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private volatile ScheduledFuture<?> myRunFuture;

    private final ScheduleManagerImpl.JobRunTask myRunTask;
    private final LockFactory myLockFactory;
    private final ScheduledExecutorService myExecutor;

    private final long myRunDelayInMs;

    private ScheduleManagerImpl(Builder builder)
    {
        myLockFactory = builder.myLockFactory;
        myRunDelayInMs = builder.myRunIntervalInMs;
        myRunTask = new JobRunTask();
        myExecutor = Executors.newSingleThreadScheduledExecutor();
        start();
    }

    public boolean addRunPolicy(RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} added", runPolicy);
        return myRunPolicies.add(runPolicy);
    }

    public boolean removeRunPolicy(RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} removed", runPolicy);
        return myRunPolicies.remove(runPolicy);
    }

    @Override
    public void schedule(ScheduledJob job)
    {
        myQueue.add(job);
    }

    @Override
    public void deschedule(ScheduledJob job)
    {
        myQueue.remove(job);
    }

    @Override
    public void close()
    {
        stop();
        myRunPolicies.clear();
    }

    /**
     * Start the run scheduler.
     */
    public void start()
    {
        myRunFuture = myExecutor.scheduleWithFixedDelay(myRunTask, myRunDelayInMs, myRunDelayInMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the run scheduler.
     */
    public void stop()
    {
        if (myRunFuture != null)
        {
            myRunFuture.cancel(false);
        }

        myExecutor.shutdown();
    }

    @VisibleForTesting
    public void run()
    {
        myRunTask.run();
    }

    @VisibleForTesting
    public int getQueueSize() { return myQueue.size(); }

    private Long validateJob(ScheduledJob job)
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
    public class JobRunTask implements Runnable
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
                if (validate(next) && tryRunTasks(next))
                {
                    break;
                }
            }
        }

        private boolean validate(ScheduledJob job)
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

        private boolean tryRunTasks(ScheduledJob next)
        {
            for (ScheduledTask task : next)
            {
                if (tryRunTask(next, task))
                {
                    return true;
                }
            }
            return false;
        }

        private boolean tryRunTask(ScheduledJob job, ScheduledTask task)
        {
            LOG.debug("Trying to acquire lock for {}", task);
            try (LockFactory.DistributedLock lock = task.getLock(myLockFactory))
            {
                boolean successful = runTask(task);
                job.postExecute(successful);
                return true;
            }
            catch (LockException e)
            {
                if (e.getCause() != null)
                {
                    LOG.warn("Unable to get schedule lock on task", task, e);
                }
                return false;
            }
        }

        private boolean runTask(ScheduledTask task)
        {
            try
            {
                LOG.info("Running scheduled task {}", task);
                return task.execute();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to run task {}", task, e);
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

        public Builder withLockFactory(LockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        public Builder withRunInterval(long runInterval, TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }


        public ScheduleManagerImpl build()
        {
            return new ScheduleManagerImpl(this);
        }
    }
}