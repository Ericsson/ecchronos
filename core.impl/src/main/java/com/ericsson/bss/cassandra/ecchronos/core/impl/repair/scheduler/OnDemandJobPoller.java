/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Periodically polls for ongoing on-demand repair jobs and schedules them.
 */
public final class OnDemandJobPoller implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(OnDemandJobPoller.class);
    private static final int ONGOING_JOBS_PERIOD_SECONDS = 10;

    private final OnDemandStatus myOnDemandStatus;
    private final ReplicationState myReplicationState;
    private final ScheduleManager myScheduleManager;
    private final OnDemandRepairJobFactory myJobFactory;
    private final Function<OnDemandRepairJob, Boolean> myTryAddJob;
    private final Runnable myRemoveFinishedJobs;

    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("OngoingJobsScheduler-%d").build());

    private OnDemandJobPoller(final Builder builder)
    {
        myOnDemandStatus = builder.myOnDemandStatus;
        myReplicationState = builder.myReplicationState;
        myScheduleManager = builder.myScheduleManager;
        myJobFactory = builder.myJobFactory;
        myTryAddJob = builder.myTryAddJob;
        myRemoveFinishedJobs = builder.myRemoveFinishedJobs;
        myExecutor.scheduleAtFixedRate(this::pollOngoingJobs, 0, ONGOING_JOBS_PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Retrieves and schedules ongoing on-demand repair jobs for a specific host.
     *
     * @param hostId The host for which to schedule ongoing jobs.
     */
    public void scheduleOngoingJobs(final UUID hostId)
    {
        try
        {
            Set<OngoingJob> ongoingJobs = myOnDemandStatus.getOngoingJobs(myReplicationState, hostId);
            ongoingJobs.forEach(this::scheduleOngoingJob);
        }
        catch (Exception e)
        {
            LOG.warn("Failed to get ongoing on demand jobs, automatic retry in {}s", ONGOING_JOBS_PERIOD_SECONDS, e);
        }
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(ONGOING_JOBS_PERIOD_SECONDS, TimeUnit.SECONDS))
            {
                myExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            myExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void pollOngoingJobs()
    {
        try
        {
            myRemoveFinishedJobs.run();
            Map<UUID, Set<OngoingJob>> allOngoingJobs = myOnDemandStatus.getOngoingStartedJobsForAllNodes(myReplicationState);
            allOngoingJobs.values().forEach(jobs -> jobs.forEach(this::scheduleOngoingJob));
        }
        catch (Exception e)
        {
            LOG.warn("Failed to get ongoing on demand jobs, automatic retry in {}s", ONGOING_JOBS_PERIOD_SECONDS, e);
        }
    }

    private void scheduleOngoingJob(final OngoingJob ongoingJob)
    {
        if (isAlreadyCompleted(ongoingJob))
        {
            LOG.debug("Skipping already completed ongoing job: {}", ongoingJob.getJobId());
            ongoingJob.finishJob();
            return;
        }
        OnDemandRepairJob job = myJobFactory.createFromOngoingJob(ongoingJob);
        if (Boolean.TRUE.equals(myTryAddJob.apply(job)))
        {
            LOG.info("Scheduling ongoing job: {}", job.getJobId());
            myScheduleManager.schedule(ongoingJob.getHostId(), job);
        }
    }

    private boolean isAlreadyCompleted(final OngoingJob ongoingJob)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokens = ongoingJob.getTokens();
        Set<LongTokenRange> repairedTokens = ongoingJob.getRepairedTokens();
        return tokens != null && !tokens.isEmpty()
                && repairedTokens != null && repairedTokens.containsAll(tokens.keySet());
    }

    /**
     * Create a new Builder instance.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder for constructing {@link OnDemandJobPoller}.
     */
    public static final class Builder
    {
        private OnDemandStatus myOnDemandStatus;
        private ReplicationState myReplicationState;
        private ScheduleManager myScheduleManager;
        private OnDemandRepairJobFactory myJobFactory;
        private Function<OnDemandRepairJob, Boolean> myTryAddJob;
        private Runnable myRemoveFinishedJobs;

        public Builder withOnDemandStatus(final OnDemandStatus onDemandStatus)
        {
            myOnDemandStatus = onDemandStatus;
            return this;
        }

        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        public Builder withScheduleManager(final ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        public Builder withJobFactory(final OnDemandRepairJobFactory jobFactory)
        {
            myJobFactory = jobFactory;
            return this;
        }

        public Builder withTryAddJob(final Function<OnDemandRepairJob, Boolean> tryAddJob)
        {
            myTryAddJob = tryAddJob;
            return this;
        }

        public Builder withRemoveFinishedJobs(final Runnable removeFinishedJobs)
        {
            myRemoveFinishedJobs = removeFinishedJobs;
            return this;
        }

        public OnDemandJobPoller build()
        {
            return new OnDemandJobPoller(this);
        }
    }
}
