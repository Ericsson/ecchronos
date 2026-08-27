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
package com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * A scheduled job that should be managed by the {@link ScheduleManager}.
 */
@SuppressWarnings("VisibilityModifier")
public abstract class ScheduledJob implements Iterable<ScheduledTask>
{
    private static final long DEFAULT_BACKOFF_IN_MINUTES = 30;

    private final Priority myPriority;
    private final long myBackoffInMs;
    /** The configured run interval in milliseconds. */
    protected final long myRunIntervalInMs;

    /** The timestamp of the last successful run in milliseconds since epoch. */
    protected volatile long myLastSuccessfulRun = -1L;
    private volatile long myNextRunTimeInMs = -1L;
    private volatile long myRunOffset = 0;
    private transient volatile boolean myFailed = false;
    private final UUID myNodeID;
    private final UUID myJobID;
    private final TimeUnit myPriorityGranularity;

    /**
     * Constructs a scheduled job with a randomly generated job ID.
     *
     * @param configuration the job configuration.
     * @param nodeID the unique identifier of the node running the job.
     */
    public ScheduledJob(final Configuration configuration, final UUID nodeID)
    {
        this(configuration, UUID.randomUUID(), nodeID);
    }

    /**
     * Constructs a scheduled job with the specified job ID.
     *
     * @param configuration the job configuration.
     * @param jobID the unique identifier for this job.
     * @param nodeID the unique identifier of the node running the job.
     */
    public ScheduledJob(final Configuration configuration, final UUID jobID, final UUID nodeID)
    {
        myNodeID = nodeID;
        myJobID = jobID;
        myPriority = configuration.priority();
        myRunIntervalInMs = configuration.runIntervalInMs();
        myBackoffInMs = configuration.backoffInMs();
        myLastSuccessfulRun = System.currentTimeMillis() - myRunIntervalInMs;
        myPriorityGranularity = configuration.priorityGranularity();
    }

    /**
     * This method gets run after the execution of one task has completed.
     * <p>
     * When overriding this method make sure to call super.postExecute(success, task) in the end.
     *
     * @param successful
     *            If the job ran successfully.
     * @param task
     *            The task that was executed.
     */
    public void postExecute(final boolean successful, final ScheduledTask task)
    {
        if (successful)
        {
            myLastSuccessfulRun = System.currentTimeMillis();
            myNextRunTimeInMs = -1L;
        }
        else
        {
            myNextRunTimeInMs = System.currentTimeMillis() + myBackoffInMs;
        }
    }

    /**
     * This method gets run after the job is removed from the Queue. It will run whether the job fails or succeeds.
     */
    public void finishJob()
    {
        // Do nothing
    }

    /**
     * This method is called every time the scheduler creates a list of jobs to run.
     * Use this if you need to do some updates before priority is calculated.
     * Default is noop.
     */
    public void refreshState()
    {
        // NOOP by default
    }

    /**
     * Set the job to be runnable again after the given delay has elapsed.
     *
     * @param delay
     *            The delay in milliseconds to wait until the job is runnable again.
     */
    public final void setRunnableIn(final long delay)
    {
        myNextRunTimeInMs = System.currentTimeMillis() + delay;
    }

    /**
     * Mark this job as permanently failed.
     * <p>
     * Once marked, {@link #getState()} will return {@link State#FAILED}
     * and the job will be descheduled by the queue on the next iteration.
     */
    public void markFailed()
    {
        myFailed = true;
    }

    /**
     * Check if this job is runnable now.
     *
     * @return True if able to run now.
     */
    public boolean runnable()
    {
        return !myFailed && myNextRunTimeInMs <= System.currentTimeMillis() && getRealPriority() > -1;
    }

    /**
     * Get current State of the job.
     *
     * @return current State
     */
    public State getState()
    {
        if (myFailed)
        {
            return State.FAILED;
        }
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
     * The current priority is calculated as the {@link #getPriority() configured priority} times the number of hours
     * that has passed since it *could* start running.
     *
     * @return The current priority or -1 if the job shouldn't run now.
     * @see #getPriority()
     */
    public int getRealPriority()
    {
        return getRealPriority(getLastSuccessfulRun());
    }

    /**
     * Get the current priority of the job based on a given last successful run timestamp.
     *
     * @param lastSuccessfulRun the timestamp of the last successful run in milliseconds since epoch.
     * @return The current priority or -1 if the job shouldn't run now.
     */
    public final int getRealPriority(final long lastSuccessfulRun)
    {
        long now = System.currentTimeMillis();

        long diff = now - (lastSuccessfulRun + myRunIntervalInMs - getRunOffset());

        if (diff < 0)
        {
            return -1;
        }

        long granularityInMs = myPriorityGranularity.toMillis(1);
        long unitsPassed = diff / granularityInMs + 1;

        // Overflow protection
        if (unitsPassed > Integer.MAX_VALUE / myPriority.getValue())
        {
            return Integer.MAX_VALUE;
        }

        return (int) unitsPassed * myPriority.getValue();
    }

    /**
     * Gets the offset for the job.
     *
     * @return The offset for the job.
     */
    public long getRunOffset()
    {
        return myRunOffset;
    }

    /**
     * Gets the unique identifier for this job.
     *
     * @return unique identifier for Job
     */
    public final UUID getJobId()
    {
        return myJobID;
    }

    /**
     * Gets the unique identifier for the node running the job.
     *
     * @return unique identifier for the node running the job.
     */
    public final UUID getNodeId()
    {
        return myNodeID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ScheduledJob that = (ScheduledJob) o;
        return myBackoffInMs == that.myBackoffInMs
                && myRunIntervalInMs == that.myRunIntervalInMs
                && myLastSuccessfulRun == that.myLastSuccessfulRun
                && myNextRunTimeInMs == that.myNextRunTimeInMs
                && myRunOffset == that.myRunOffset
                && myPriority == that.myPriority
                && Objects.equals(myJobID, that.myJobID)
                && Objects.equals(myNodeID, that.myNodeID)
                && Objects.equals(myPriorityGranularity, that.myPriorityGranularity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myPriority, myBackoffInMs, myRunIntervalInMs, myLastSuccessfulRun,
                myNextRunTimeInMs, myRunOffset, myJobID, myNodeID, myPriorityGranularity);
    }

    /**
     * The different priorities a job can have.
     * <p>
     * The higher the value a job has the more the {@link ScheduledJob#getRealPriority() current priority} is increased
     * each hour.
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

        Priority(final int aValue)
        {
            this.value = aValue;
        }

        /**
         * Gets the numeric value of this priority level.
         *
         * @return the priority value.
         */
        public int getValue()
        {
            return value;
        }
    }

    /**
     * Represents the possible states of a scheduled job.
     */
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
        FAILED,

        /**
         * The Job is blocked by policies and cannot be run.
         */
        BLOCKED
    }

    /**
     * The configuration of a job.
     *
     * @param priority the priority level of the job.
     * @param runIntervalInMs the interval between runs in milliseconds.
     * @param backoffInMs the backoff time in milliseconds after a failed run.
     * @param priorityGranularity the time unit used for priority increment calculations.
     */
    public record Configuration(Priority priority, long runIntervalInMs, long backoffInMs, TimeUnit priorityGranularity)
    {
    }

    /**
     * Builder class for the {@link Configuration}.
     */
    public static class ConfigurationBuilder
    {
        /**
         * Default constructor.
         */
        public ConfigurationBuilder()
        {
            // Default constructor
        }

        private Priority priority = Priority.LOW;
        private long runIntervalInMs = TimeUnit.DAYS.toMillis(1);
        private long backoffInMs = TimeUnit.MINUTES.toMillis(DEFAULT_BACKOFF_IN_MINUTES);
        private TimeUnit granularityUnit = TimeUnit.HOURS;

        /**
         * Sets the time unit used to calculate priority increments.
         *
         * @param granularityTimeUnit the time unit for priority granularity.
         * @return this builder.
         */
        public final ConfigurationBuilder withPriorityGranularity(final TimeUnit granularityTimeUnit)
        {
            this.granularityUnit = granularityTimeUnit;
            return this;
        }

        /**
         * Sets the priority of the job.
         *
         * @param aPriority the priority level.
         * @return this builder.
         */
        public final ConfigurationBuilder withPriority(final Priority aPriority)
        {
            this.priority = aPriority;
            return this;
        }

        /**
         * Sets the run interval of the job.
         *
         * @param runInterval the interval value.
         * @param unit the time unit of the interval.
         * @return this builder.
         */
        public final ConfigurationBuilder withRunInterval(final long runInterval, final TimeUnit unit)
        {
            this.runIntervalInMs = unit.toMillis(runInterval);
            return this;
        }

        /**
         * Sets the backoff time after a failed run.
         *
         * @param backoff the backoff value.
         * @param unit the time unit of the backoff.
         * @return this builder.
         */
        public final ConfigurationBuilder withBackoff(final long backoff, final TimeUnit unit)
        {
            this.backoffInMs = unit.toMillis(backoff);
            return this;
        }

        /**
         * Builds the {@link Configuration} instance.
         *
         * @return the constructed configuration.
         */
        public final Configuration build()
        {
            return new Configuration(priority, runIntervalInMs, backoffInMs, granularityUnit);
        }
    }
}

