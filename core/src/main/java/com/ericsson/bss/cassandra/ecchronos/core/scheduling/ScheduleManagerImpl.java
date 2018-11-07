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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleManager handles the run scheduler and update scheduler.
 */
public class ScheduleManagerImpl implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerImpl.class);

    private final ScheduledJobQueue myQueue = new ScheduledJobQueue(new DefaultJobComparator());
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final RunScheduler myScheduler;

    private ScheduleManagerImpl(Builder builder)
    {
        myScheduler = RunScheduler.builder()
                .withQueue(myQueue)
                .withLockFactory(builder.myLockFactory)
                .withRunPolicy(this::validateJob)
                .withRunDelay(builder.myRunIntervalInMs, TimeUnit.MILLISECONDS)
                .build();
        myScheduler.start();
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
        myScheduler.stop();
        myRunPolicies.clear();
    }

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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private LockFactory myLockFactory;
        private long myRunIntervalInMs = RunScheduler.DEFAULT_RUN_DELAY_IN_MS;

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
