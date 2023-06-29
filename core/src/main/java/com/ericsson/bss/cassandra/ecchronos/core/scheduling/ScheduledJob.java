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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A scheduled job that should be managed by the {@link ScheduleManager}.
 */
public abstract class ScheduledJob implements Iterable<ScheduledTask>
{
    public static final long DEFAULT_WAIT_BETWEEN_UNSUCCESSFUL_RUNS_IN_MILLISECONDS = TimeUnit.MINUTES.toMillis(30);

    private final Priority myPriority;
    protected final long myRunIntervalInMs;

    protected volatile long myLastSuccessfulRun = -1;
    private volatile long myNextRunTime = -1;
    private final UUID myId;

    public ScheduledJob(Configuration configuration)
    {
        this(configuration, UUID.randomUUID());
    }

    public ScheduledJob(Configuration configuration, UUID id)
    {
        myId = id;
        myPriority = configuration.priority;
        myRunIntervalInMs = configuration.runIntervalInMs;
        myLastSuccessfulRun = System.currentTimeMillis() - myRunIntervalInMs;
    }

    /**
     * This method gets run after the execution of one task has completed.
     * <p>
     * When overriding this method make sure to call super.postExecute(success, task) in the end.
     *
     * @param successful
     *            If the job ran successfully.
     * @param task
     *            Last task that has completely successful
     */
    protected void postExecute(boolean successful, ScheduledTask task)
    {
        if (successful)
        {
            myLastSuccessfulRun = System.currentTimeMillis();
            myNextRunTime = -1;
        }
        else
        {
            myNextRunTime = System.currentTimeMillis() + DEFAULT_WAIT_BETWEEN_UNSUCCESSFUL_RUNS_IN_MILLISECONDS;
        }
    }

    /**
     * This method gets run after the job is removed from the Queue. It will run whether the job fails or succeeds.
     */
    protected void finishJob() {}

    /**
     * Set the job to be runnable again after the given delay has elapsed.
     *
     * @param delay
     *            The delay in milliseconds to wait until the job is runnable again.
     */
    public final void setRunnableIn(long delay)
    {
        myNextRunTime = System.currentTimeMillis() + delay;
    }

    /**
     * Check if this job is runnable now.
     *
     * @return True if able to run now.
     */
    public boolean runnable()
    {
        return myNextRunTime <= System.currentTimeMillis() && getRealPriority() > -1;
    }

    /**
     * Get current State of the job.
     *
     * @return current State
     */
    public State getState()
    {
        if (runnable())
        {
            return State.RUNNABLE;
        }
        return State.PARKED;
    }

    /**
     * Get the unix timestamp of the last time this job was run.
     *
     * @return The last time the job ran successfully.
     */
    public long getLastSuccessfulRun()
    {
        return myLastSuccessfulRun;
    }

    /**
     * Get the configured priority of this job.
     *
     * @return The priority of this job.
     * @see #getRealPriority()
     */
    public Priority getPriority()
    {
        return myPriority;
    }

    /**
     * Get the current priority of the job.
     * <p>
     * The current priority is calculated as the {@link #getPriority() configured priority} times the number of hours that has passed since it *could*
     * start running.
     *
     * @return The current priority or -1 if the job shouldn't run now.
     * @see #getPriority()
     */
    public int getRealPriority()
    {
        return getRealPriority(getLastSuccessfulRun());
    }

    public final int getRealPriority(final long lastSuccessfulRun)
    {
        long now = System.currentTimeMillis();

        long diff = now - (lastSuccessfulRun + myRunIntervalInMs);

        if (diff < 0)
        {
            return -1;
        }

        int hours = (int) (diff / 3600000) + 1;

        return hours * myPriority.getValue();
    }

    /**
     * @return unique identifier for Job
     */
    public final UUID getId()
    {
        return myId;
    }

    /**
     * The different priorities a job can have.
     * <p>
     * The higher the value a job has the more the {@link ScheduledJob#getRealPriority() current priority} is increased each hour.
     */
    public enum Priority
    {
        /**
         * Low priority, steps the current priority by 1 each hour.
         */
        LOW(1),

        /**
         * Medium priority, steps the current priority by 2 each hour.
         */
        MEDIUM(2),

        /**
         * High priority, steps the current priority by 3 each hour.
         */
        HIGH(3),

        /**
         * Highest priority, steps the current priority by 100 each hour.
         * <p>
         * Should be used later on for user defined operations.
         */
        HIGHEST(100);

        private final int value;

        Priority(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }
    }

    public enum State
    {
        /**
         * Job is pending to be run.
         */
        RUNNABLE,

        /**
         * Job is finished and can be discarded.
         */
        FINISHED,

        /**
         * The Job cannot be run currently.
         */
        PARKED,

        /**
         * The Job has failed and can be discarded.
         */
        FAILED
    }

    /**
     * The configuration of a job.
     */
    public static class Configuration
    {
        /**
         * The priority of the job.
         */
        public final Priority priority;

        /**
         * The minimum amount of time to wait between each successful run.
         */
        public final long runIntervalInMs;

        Configuration(ConfigurationBuilder builder)
        {
            priority = builder.priority;
            runIntervalInMs = builder.runIntervalInMs;
        }
    }

    /**
     * Builder class for the {@link Configuration}.
     */
    public static class ConfigurationBuilder
    {
        private Priority priority = Priority.LOW;
        private long runIntervalInMs = TimeUnit.DAYS.toMillis(1);

        public ConfigurationBuilder withPriority(Priority priority)
        {
            this.priority = priority;
            return this;
        }

        public ConfigurationBuilder withRunInterval(long runInterval, TimeUnit unit)
        {
            this.runIntervalInMs = unit.toMillis(runInterval);
            return this;
        }

        public Configuration build()
        {
            return new Configuration(this);
        }
    }
}
