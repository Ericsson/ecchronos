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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.google.common.annotations.VisibleForTesting;

/**
 * Handles the running of the scheduled jobs.
 * <p>
 * Periodically wakes up to see if there are any jobs that needs to be run.
 */
public class RunScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(RunScheduler.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);

    private final JobRunTask myRunTask;
    private final ScheduledJobQueue myQueue;
    private final LockFactory myLockFactory;
    private final ScheduledExecutorService myExecutor;
    private final Function<ScheduledJob, Long> myRunPolicy;

    private volatile ScheduledFuture<?> myRunFuture;

    private final long myRunDelayInMs;

    private RunScheduler(Builder builder)
    {
        myQueue = builder.myQueue;
        myLockFactory = builder.myLockFactory;
        myRunPolicy = builder.myRunPolicy;
        myRunDelayInMs = builder.myRunDelayInMs;
        myRunTask = new JobRunTask();

        myExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Start the run scheduler.
     */
    public void start()
    {
        myRunFuture = myExecutor.scheduleWithFixedDelay(myRunTask, myRunDelayInMs, myRunDelayInMs, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    public void run()
    {
        myRunTask.run();
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

    /**
     * Internal run task that is scheduled by the {@link RunScheduler}.
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
                    tryRunJob(next);
                    break;
                }
            }
        }

        private boolean validate(ScheduledJob job)
        {
            LOG.trace("Validating job {}", job);
            long nextRun = myRunPolicy.apply(job);

            if (nextRun != -1)
            {
                job.setRunnableIn(nextRun);
                return false;
            }

            return true;
        }

        private void tryRunJob(ScheduledJob job)
        {
            LOG.debug("Trying to acquire lock for {}", job);
            try (LockFactory.DistributedLock lock = job.getLock(myLockFactory))
            {
                runJob(job);
            }
            catch (LockException e)
            {
                if (e.getCause() != null)
                {
                    LOG.warn("Unable to get schedule lock on job", job, e);
                }
            }
        }

        private void runJob(ScheduledJob job)
        {
            try
            {
                LOG.info("Running scheduled job {}", job);
                job.execute();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to run job {}", job, e);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ScheduledJobQueue myQueue;
        private LockFactory myLockFactory;
        private Function<ScheduledJob, Long> myRunPolicy;
        private long myRunDelayInMs = DEFAULT_RUN_DELAY_IN_MS;

        public Builder withQueue(ScheduledJobQueue queue)
        {
            myQueue = queue;
            return this;
        }

        public Builder withLockFactory(LockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        public Builder withRunPolicy(Function<ScheduledJob, Long> runPolicy)
        {
            myRunPolicy = runPolicy;
            return this;
        }

        public Builder withRunDelay(long runDelay, TimeUnit timeUnit)
        {
            myRunDelayInMs = timeUnit.toMillis(runDelay);
            return this;
        }

        public RunScheduler build()
        {
            return new RunScheduler(this);
        }
    }
}
