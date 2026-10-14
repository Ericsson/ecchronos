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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Handles the per-task execution logic for a single node's repair session.
 * Extracted from {@link ScheduleManagerImpl.JobRunTask} to reduce its complexity.
 */
final class JobRunExecutor
{
    private static final Logger LOG = LoggerFactory.getLogger(JobRunExecutor.class);

    private final UUID nodeID;
    private final Node myNode;
    private final LockFailureBackoff myLockFailureBackoff;
    private final SchedulerMetrics mySchedulerMetrics;
    private final ConcurrentHashMap<UUID, AtomicInteger> myConsecutiveFailures;
    private final BiFunction<ScheduledJob, Node, Long> jobValidator;

    JobRunExecutor(
            final UUID currentNodeID,
            final Node node,
            final LockFailureBackoff lockFailureBackoff,
            final SchedulerMetrics schedulerMetrics,
            final ConcurrentHashMap<UUID, AtomicInteger> consecutiveFailures,
            final BiFunction<ScheduledJob, Node, Long> validator)
    {
        this.nodeID = currentNodeID;
        this.myNode = node;
        this.myLockFailureBackoff = lockFailureBackoff;
        this.mySchedulerMetrics = schedulerMetrics;
        this.myConsecutiveFailures = consecutiveFailures;
        this.jobValidator = validator;
    }

    int executeJobTasks(
            final ScheduledJob job,
            final long sessionStart,
            final SessionLockPool lockPool,
            final long sessionWindowInMs,
            final boolean forceRefresh)
    {
        AtomicInteger failureCounter = myConsecutiveFailures.computeIfAbsent(
                job.getJobId(), k -> new AtomicInteger(0));

        if (!forceRefresh)
        {
            // Backward-compatible behaviour (default): drain the job's current snapshot once, then do a single
            // throttled refresh if work was done so the next tick sees up-to-date state.
            int executed = drainTaskSnapshot(job, sessionStart, lockPool, failureCounter, sessionWindowInMs)
                    .tasksExecuted();
            if (executed > 0)
            {
                job.refreshState();
            }
            return executed;
        }

        int totalExecuted = 0;
        // Force-refresh re-drive (opt-in via scheduler.lock_session.force_refresh). Drain the job's current task
        // snapshot; if the whole snapshot was drained within the session window and work was done, force a
        // repair-state refresh and re-drive the job while it still reports repairable work. This avoids leaving the
        // node idle until the next (throttled, ~14 min for a 24h interval) refresh when there is more work to do
        // now. The forced refresh happens at most once per drained snapshot - roughly once per ~tens of tasks, not
        // per task - so the O(ranges) recompute cost stays amortised.
        //
        // The loop is bounded by a fixed iteration count (MAX_SESSION_REFRESH_REDRIVES + 1 passes) so it can never
        // spin indefinitely; it also stops early as soon as the window closes, a drain is cut short by the window,
        // no task runs, or the job reports no repairable work (empty snapshot). The first pass is the initial
        // drain; each subsequent pass is a force-refresh re-drive.
        boolean forcedRefreshed = false;
        for (int pass = 0; pass <= ScheduleManagerImpl.MAX_SESSION_REFRESH_REDRIVES; pass++)
        {
            DrainResult result = drainTaskSnapshot(job, sessionStart, lockPool, failureCounter, sessionWindowInMs);
            totalExecuted += result.tasksExecuted();

            boolean stop = result.tasksExecuted() == 0
                    || result.windowTruncated()
                    || !withinSessionWindow(sessionStart, sessionWindowInMs);
            if (!stop)
            {
                // Snapshot fully drained within the window and work was done: force a refresh and continue only if
                // there are more repairable ranges to pick up this session.
                job.refreshState(true);
                forcedRefreshed = true;
                if (!job.hasRepairableWork())
                {
                    stop = true;
                }
            }
            if (stop)
            {
                // If we never forced a refresh this call, do a final throttled refresh (when work was done) so the
                // next tick sees up-to-date state, matching the prior behaviour. A forced refresh already updated
                // the state, so no extra refresh is needed in that case.
                if (!forcedRefreshed && totalExecuted > 0)
                {
                    job.refreshState();
                }
                break;
            }
        }
        return totalExecuted;
    }

    private DrainResult drainTaskSnapshot(
            final ScheduledJob job,
            final long sessionStart,
            final SessionLockPool lockPool,
            final AtomicInteger failureCounter,
            final long sessionWindowInMs)
    {
        int tasksExecuted = 0;
        int index = 0;
        boolean windowTruncated = false;
        List<ScheduledTask> tasksForSession = new ArrayList<>();
        job.iterator().forEachRemaining(tasksForSession::add);
        for (ScheduledTask task : tasksForSession)
        {
            if (!withinSessionWindow(sessionStart, sessionWindowInMs))
            {
                // Stopped before consuming the whole snapshot: there is more backlog but no time left this pass.
                windowTruncated = true;
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
        return new DrainResult(tasksExecuted, windowTruncated);
    }

    /**
     * Outcome of draining one task snapshot.
     *
     * @param tasksExecuted number of tasks executed successfully.
     * @param windowTruncated whether the pass stopped because the session window elapsed with tasks still pending.
     */
    private record DrainResult(int tasksExecuted, boolean windowTruncated)
    {
    }

    boolean handleUnsuccessfulTask(final ScheduledJob job, final AtomicInteger failureCounter)
    {
        long rejectDelay = jobValidator.apply(job, myNode);
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

    static boolean withinSessionWindow(final long sessionStart, final long sessionWindowInMs)
    {
        return (System.currentTimeMillis() - sessionStart) < sessionWindowInMs;
    }
}
