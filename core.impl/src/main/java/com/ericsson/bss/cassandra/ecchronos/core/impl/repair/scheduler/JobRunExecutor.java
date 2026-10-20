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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Handles the per-task execution logic for a single node's repair session.
 * Extracted from {@link ScheduleManagerImpl.JobRunTask} to reduce its complexity.
 */
final class JobRunExecutor
{
    private static final Logger LOG = LoggerFactory.getLogger(JobRunExecutor.class);

    private final UUID nodeID;
    // Resolve the node lazily on each use instead of capturing it once at construction. The node may not yet be
    // registered in the native connection provider when this executor is created (see issue #1754), and a node's
    // endpoint can change across its lifetime; reading it on demand avoids holding a stale or null reference.
    private final Supplier<Node> myNodeSupplier;
    private final LockFailureBackoff myLockFailureBackoff;
    private final SchedulerMetrics mySchedulerMetrics;
    private final ConcurrentHashMap<UUID, AtomicInteger> myConsecutiveFailures;
    private final BiFunction<ScheduledJob, Node, Long> jobValidator;
    // Shared time source owned by ScheduleManagerImpl. Session-window checks read from this so that tests
    // can drive timing deterministically by swapping the clock; defaults to the system clock in production.
    private final AtomicReference<Clock> myClock;

    JobRunExecutor(
            final UUID currentNodeID,
            final Supplier<Node> nodeSupplier,
            final LockFailureBackoff lockFailureBackoff,
            final SchedulerMetrics schedulerMetrics,
            final ConcurrentHashMap<UUID, AtomicInteger> consecutiveFailures,
            final BiFunction<ScheduledJob, Node, Long> validator,
            final AtomicReference<Clock> clock)
    {
        this.nodeID = currentNodeID;
        this.myNodeSupplier = nodeSupplier;
        this.myLockFailureBackoff = lockFailureBackoff;
        this.mySchedulerMetrics = schedulerMetrics;
        this.myConsecutiveFailures = consecutiveFailures;
        this.jobValidator = validator;
        this.myClock = clock;
    }

    int executeJobTasks(
            final ScheduledJob job,
            final long sessionStart,
            final SessionLockPool lockPool,
            final long sessionWindowInMs)
    {
        AtomicInteger failureCounter = myConsecutiveFailures.computeIfAbsent(
                job.getJobId(), k -> new AtomicInteger(0));
        int tasksExecuted = drainTaskSnapshot(job, sessionStart, lockPool, failureCounter, sessionWindowInMs);
        if (tasksExecuted > 0)
        {
            job.refreshState();
        }
        return tasksExecuted;
    }

    private int drainTaskSnapshot(
            final ScheduledJob job,
            final long sessionStart,
            final SessionLockPool lockPool,
            final AtomicInteger failureCounter,
            final long sessionWindowInMs)
    {
        int tasksExecuted = 0;
        int index = 0;
        List<ScheduledTask> tasksForSession = new ArrayList<>();
        job.iterator().forEachRemaining(tasksForSession::add);
        for (ScheduledTask task : tasksForSession)
        {
            if (!withinSessionWindow(sessionStart, sessionWindowInMs))
            {
                break;
            }
            index++;
            long lockStart = System.nanoTime();
            try
            {
                lockPool.acquireForTask(task);
                mySchedulerMetrics.recordLockAttempt(true, System.nanoTime() - lockStart);
                TaskExecutionResult result = runTask(task, index);
                job.postExecute(result, task);
                if (result.isSuccessful())
                {
                    tasksExecuted++;
                    failureCounter.set(0);
                    myLockFailureBackoff.reset(job);
                }
                else if (result.isRetryable())
                {
                    continue;
                }
                else if (handleUnsuccessfulTask(job, failureCounter))
                {
                    break;
                }
            }
            catch (Exception e)
            {
                mySchedulerMetrics.recordLockAttempt(false, System.nanoTime() - lockStart);
                handleLockFailure(job, e);
            }
            finally
            {
                lockPool.releaseTaskLocks();
            }
        }
        return tasksExecuted;
    }

    boolean handleUnsuccessfulTask(final ScheduledJob job, final AtomicInteger failureCounter)
    {
        long rejectDelay = jobValidator.apply(job, myNodeSupplier.get());
        if (rejectDelay != -1L)
        {
            job.setRunnableIn(rejectDelay);
            failureCounter.set(0);
            LOG.debug("Job {} on node {} stopped by run policy, parking for {} ms", job, nodeID, rejectDelay);
            return true;
        }
        int failures = failureCounter.incrementAndGet();
        if (failures >= ScheduleManagerImpl.MAX_CONSECUTIVE_TASK_FAILURES)
        {
            LOG.error("Job {} on node {} has failed {} consecutive task executions, marking as FAILED",
                    job, nodeID, failures);
            job.markFailed();
            return true;
        }
        return false;
    }

    void handleLockFailure(final ScheduledJob job, final Exception e)
    {
        myLockFailureBackoff.recordFailure(job, e, nodeID);
    }

    private TaskExecutionResult runTask(final ScheduledTask task, final int index)
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
        return TaskExecutionResult.TERMINAL;
    }

    boolean withinSessionWindow(final long sessionStart, final long sessionWindowInMs)
    {
        return (myClock.get().millis() - sessionStart) < sessionWindowInMs;
    }
}
