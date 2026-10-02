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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairLockFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.DefaultJobComparator;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduledJobQueue;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.SessionLockPool;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.RuntimeMBeanException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The consolidated schedule manager for {@code UNIFIED_VNODE} repairs.
 * <p>
 * Unlike the legacy per-node {@code ScheduleManagerImpl}, this uses a <b>single</b> global
 * {@link ScheduledJobQueue} and a <b>single</b> dispatch loop. When a {@link UnifiedTableRepairJob} runs, its
 * tasks are grouped by target node and each {@code (job, node)} unit is submitted to an elastic worker pool
 * that holds a per-node {@link SessionLockPool}. Repair concurrency is governed entirely by the distributed
 * CAS locks ({@code locksPerResource}), not by a thread-per-node: a unit that cannot acquire its lock is
 * skipped with a short backoff and retried soon.
 */
public final class UnifiedScheduleManager implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(UnifiedScheduleManager.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);
    public static final int DEFAULT_KEEP_ALIVE_TIME = 60;
    public static final int DEFAULT_TIMEOUT = 5;
    static final int MAX_CONSECUTIVE_TASK_FAILURES = 5;
    /** Worker-pool safety cap (repair concurrency is bounded by the CAS lock, not this). */
    static final int WORKER_POOL_SAFETY_CAP = 256;
    /** Sentinel for an unbounded max concurrency (worker pool bounded only by {@link #WORKER_POOL_SAFETY_CAP}). */
    static final int UNBOUNDED_CONCURRENCY = 0;
    static final long LOCK_CONTENTION_BACKOFF_MIN_MS = TimeUnit.SECONDS.toMillis(1);
    static final long LOCK_CONTENTION_BACKOFF_MAX_MS = TimeUnit.SECONDS.toMillis(5);
    static final long ACTIVE_DISPATCH_DELAY_MS = TimeUnit.SECONDS.toMillis(1);

    private final ScheduledJobQueue myQueue = new ScheduledJobQueue(new DefaultJobComparator());
    private final ConcurrentHashMap<JobNodeKey, ScheduledJob> currentExecutingJobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<JobNodeKey, Long> myContentionBackoff = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<JobNodeKey, AtomicInteger> myConsecutiveFailures = new ConcurrentHashMap<>();
    private final Set<JobNodeKey> myInFlightUnits = Sets.newConcurrentHashSet();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final CASLockFactory myLockFactory;
    private final RepairLockFactoryImpl myRepairLockFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private final ScheduledThreadPoolExecutor myExecutor;
    private final ThreadPoolExecutor myWorkerPool;
    private final DispatchTask myDispatchTask = new DispatchTask();
    private volatile ScheduledFuture<?> myDispatchFuture;
    private volatile boolean myDispatchStarted;
    private volatile long myCooldownUntil;

    private final long myRunIntervalInMs;
    private volatile long mySessionWindowInMs;
    private volatile long myCooldownInMs;
    private volatile int myMaxConcurrency = UNBOUNDED_CONCURRENCY;

    private record JobNodeKey(UUID jobId, UUID nodeId)
    {
    }

    private record DispatchOutcome(int tasksExecuted, boolean pendingRunnable)
    {
    }

    private UnifiedScheduleManager(final Builder builder)
    {
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        myExecutor = new ScheduledThreadPoolExecutor(
                1, new ThreadFactoryBuilder().setNameFormat("UnifiedDispatch-%d").build());
        myExecutor.setKeepAliveTime(DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS);
        myExecutor.allowCoreThreadTimeOut(true);
        myWorkerPool = new ThreadPoolExecutor(
                0, WORKER_POOL_SAFETY_CAP, DEFAULT_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("UnifiedTaskExecutor-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        myLockFactory = builder.myLockFactory;
        myRepairLockFactory = new RepairLockFactoryImpl();
        myRunIntervalInMs = builder.myRunIntervalInMs;
        mySessionWindowInMs = builder.mySessionWindowInMs;
        myCooldownInMs = builder.myCooldownInMs;
    }

    @Override
    public void createScheduleFutureForNodeIDList(final Collection<UUID> nodeIDList)
    {
        ensureDispatchScheduled();
    }

    @Override
    public void createScheduleFutureForNode(final UUID nodeID)
    {
        ensureDispatchScheduled();
    }

    private synchronized void ensureDispatchScheduled()
    {
        if (myDispatchStarted)
        {
            return;
        }
        myDispatchStarted = true;
        myDispatchFuture = myExecutor.schedule(myDispatchTask, myRunIntervalInMs, TimeUnit.MILLISECONDS);
        LOG.debug("Unified single dispatch loop scheduled");
    }

    @Override
    public String getCurrentJobStatus()
    {
        Set<Map.Entry<JobNodeKey, ScheduledJob>> jobs = currentExecutingJobs.entrySet();
        if (jobs.isEmpty())
        {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (Map.Entry<JobNodeKey, ScheduledJob> job : jobs)
        {
            result.append(job.getValue().getJobId())
                    .append(" on node ")
                    .append(job.getKey().nodeId())
                    .append(", ");
        }
        return result.toString();
    }

    @Override
    public long getSessionWindowInMs()
    {
        return mySessionWindowInMs;
    }

    @Override
    public void setSessionWindowInMs(final long sessionWindowInMs)
    {
        if (sessionWindowInMs <= 0)
        {
            throw new IllegalArgumentException("session_window must be > 0");
        }
        mySessionWindowInMs = sessionWindowInMs;
    }

    @Override
    public long getCooldownInMs()
    {
        return myCooldownInMs;
    }

    @Override
    public void setCooldownInMs(final long cooldownInMs)
    {
        if (cooldownInMs < 0)
        {
            throw new IllegalArgumentException("cooldown must be >= 0");
        }
        myCooldownInMs = cooldownInMs;
    }

    @Override
    public int getLocksPerResource()
    {
        return RepairLockFactoryImpl.getLocksPerResource();
    }

    @Override
    public void setLocksPerResource(final int locksPerResource)
    {
        RepairLockFactoryImpl.configure(locksPerResource);
    }

    @Override
    public int getMaxConcurrency()
    {
        return myMaxConcurrency;
    }

    @Override
    public void setMaxConcurrency(final int maxConcurrency)
    {
        myMaxConcurrency = maxConcurrency < 1 ? UNBOUNDED_CONCURRENCY : maxConcurrency;
        // Bound the elastic worker pool. 0/unbounded falls back to the safety cap; a positive value caps the
        // number of concurrently dispatched repair units. setMaximumPoolSize applies to subsequently submitted
        // units; in-flight units are not interrupted.
        int poolMax = myMaxConcurrency == UNBOUNDED_CONCURRENCY
                ? WORKER_POOL_SAFETY_CAP
                : Math.min(myMaxConcurrency, WORKER_POOL_SAFETY_CAP);
        myWorkerPool.setMaximumPoolSize(poolMax);
        LOG.info("Unified scheduler max concurrency set to {} (worker pool max now {})",
                getMaxConcurrency(), poolMax);
    }

    /**
     * Add a run policy.
     *
     * @param runPolicy the run policy to add.
     * @return {@code true} if added.
     */
    public boolean addRunPolicy(final RunPolicy runPolicy)
    {
        return myRunPolicies.add(runPolicy);
    }

    /**
     * Remove a run policy.
     *
     * @param runPolicy the run policy to remove.
     * @return {@code true} if removed.
     */
    public boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        return myRunPolicies.remove(runPolicy);
    }

    @Override
    public void schedule(final UUID nodeID, final ScheduledJob job)
    {
        myQueue.add(job);
        ensureDispatchScheduled();
    }

    @Override
    public void deschedule(final UUID nodeID, final ScheduledJob job)
    {
        myQueue.remove(job);
        myContentionBackoff.keySet().removeIf(key -> key.jobId().equals(job.getJobId()));
        myConsecutiveFailures.keySet().removeIf(key -> key.jobId().equals(job.getJobId()));
    }

    @Override
    public void removeScheduleFutureForNode(final UUID nodeID)
    {
        LOG.debug("removeScheduleFutureForNode({}) is a no-op under the unified single dispatch loop", nodeID);
    }

    @Override
    public void close()
    {
        ScheduledFuture<?> dispatchFuture = myDispatchFuture;
        if (dispatchFuture != null)
        {
            dispatchFuture.cancel(false);
        }
        myExecutor.shutdown();
        myWorkerPool.shutdown();
        awaitTermination(myExecutor);
        awaitTermination(myWorkerPool);
        myInFlightUnits.clear();
        currentExecutingJobs.clear();
        myContentionBackoff.clear();
        myConsecutiveFailures.clear();
        myRunPolicies.clear();
    }

    private void awaitTermination(final ExecutorService executor)
    {
        try
        {
            if (!executor.awaitTermination(DEFAULT_TIMEOUT, TimeUnit.MINUTES))
            {
                executor.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Made available for testing. Drives one dispatch tick synchronously (units run inline).
     */
    @VisibleForTesting
    public void run()
    {
        myDispatchTask.runOnce(false);
    }

    /**
     * Made available for testing.
     *
     * @return the global queue size.
     */
    @VisibleForTesting
    public int getQueueSize()
    {
        return myQueue.size();
    }

    private Long validateJob(final ScheduledJob job, final Node node)
    {
        for (RunPolicy runPolicy : myRunPolicies)
        {
            long nextRun = runPolicy.validate(job, node);
            if (nextRun != -1L)
            {
                return nextRun;
            }
        }
        return -1L;
    }

    private Node nodeFor(final UUID nodeId)
    {
        return nodeId == null ? null : myNativeConnectionProvider.getNodes().get(nodeId);
    }

    private final class DispatchTask implements Runnable
    {
        @Override
        public void run()
        {
            DispatchOutcome outcome = runOnce(true);
            reschedule(outcome);
        }

        private DispatchOutcome runOnce(final boolean async)
        {
            if (System.currentTimeMillis() < myCooldownUntil)
            {
                return new DispatchOutcome(0, false);
            }
            long sessionStart = System.currentTimeMillis();
            List<Future<Integer>> futures = new ArrayList<>();
            int inlineTasks = 0;
            boolean pendingRunnable = false;
            try
            {
                for (ScheduledJob job : myQueue)
                {
                    if (!withinSessionWindow(sessionStart))
                    {
                        break;
                    }
                    DispatchOutcome jobOutcome = dispatchJob(job, sessionStart, async, futures);
                    inlineTasks += jobOutcome.tasksExecuted();
                    pendingRunnable = pendingRunnable || jobOutcome.pendingRunnable();
                }
            }
            catch (Exception e)
            {
                LOG.error("Exception while dispatching unified jobs", e);
            }
            int tasksExecuted = async ? awaitUnits(futures) : inlineTasks;
            applyCooldown(tasksExecuted);
            return new DispatchOutcome(tasksExecuted, pendingRunnable);
        }

        private DispatchOutcome dispatchJob(final ScheduledJob job, final long sessionStart, final boolean async,
                final List<Future<Integer>> futures)
        {
            int inlineTasks = 0;
            boolean pendingRunnable = false;
            for (Map.Entry<UUID, List<ScheduledTask>> entry : groupTasksByNode(job).entrySet())
            {
                if (!withinSessionWindow(sessionStart))
                {
                    break;
                }
                UUID unitNode = entry.getKey();
                if (isOnLockBackoff(job, unitNode))
                {
                    pendingRunnable = true;
                    continue;
                }
                if (!validate(job, unitNode))
                {
                    continue;
                }
                JobNodeKey key = new JobNodeKey(job.getJobId(), unitNode);
                if (!myInFlightUnits.add(key))
                {
                    pendingRunnable = true;
                    continue;
                }
                RepairUnit unit = new RepairUnit(job, unitNode, entry.getValue(), sessionStart, key);
                if (async)
                {
                    futures.add(myWorkerPool.submit(unit::run));
                }
                else
                {
                    inlineTasks += unit.run();
                }
            }
            return new DispatchOutcome(inlineTasks, pendingRunnable);
        }

        private int awaitUnits(final List<Future<Integer>> futures)
        {
            int total = 0;
            for (Future<Integer> future : futures)
            {
                try
                {
                    total += future.get();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while awaiting repair unit completion", e);
                }
                catch (Exception e)
                {
                    LOG.warn("Repair unit failed", e);
                }
            }
            return total;
        }

        private void reschedule(final DispatchOutcome outcome)
        {
            long delay;
            if (System.currentTimeMillis() < myCooldownUntil)
            {
                delay = myCooldownUntil - System.currentTimeMillis();
            }
            else if (outcome.tasksExecuted() > 0 || outcome.pendingRunnable())
            {
                delay = ACTIVE_DISPATCH_DELAY_MS;
            }
            else
            {
                delay = myRunIntervalInMs;
            }
            try
            {
                myDispatchFuture = myExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
            catch (RejectedExecutionException e)
            {
                LOG.debug("Unified scheduler shutting down, not rescheduling dispatch");
            }
        }

        private Map<UUID, List<ScheduledTask>> groupTasksByNode(final ScheduledJob job)
        {
            Map<UUID, List<ScheduledTask>> tasksByNode = new LinkedHashMap<>();
            for (ScheduledTask task : job)
            {
                UUID taskNode = task.getNodeId();
                if (taskNode == null)
                {
                    taskNode = job.getNodeId();
                }
                tasksByNode.computeIfAbsent(taskNode, k -> new ArrayList<>()).add(task);
            }
            return tasksByNode;
        }

        private boolean isOnLockBackoff(final ScheduledJob job, final UUID unitNode)
        {
            Long backoffUntil = myContentionBackoff.get(new JobNodeKey(job.getJobId(), unitNode));
            return backoffUntil != null && System.currentTimeMillis() < backoffUntil;
        }

        private boolean validate(final ScheduledJob job, final UUID unitNode)
        {
            long nextRun = validateJob(job, nodeFor(unitNode));
            if (nextRun != -1L)
            {
                job.setRunnableIn(nextRun);
                return false;
            }
            return true;
        }

        private void applyCooldown(final int tasksExecuted)
        {
            if (tasksExecuted > 0 && myCooldownInMs > 0)
            {
                myCooldownUntil = System.currentTimeMillis() + myCooldownInMs;
            }
        }

        private boolean withinSessionWindow(final long sessionStart)
        {
            return (System.currentTimeMillis() - sessionStart) < mySessionWindowInMs;
        }
    }

    /**
     * One unit of work: the tasks of one job targeting one node, run sequentially reusing one
     * {@link SessionLockPool} for that node.
     */
    private final class RepairUnit
    {
        private final ScheduledJob myJob;
        private final UUID myNodeId;
        private final List<ScheduledTask> myTasks;
        private final long mySessionStart;
        private final JobNodeKey myKey;

        RepairUnit(final ScheduledJob job, final UUID nodeId, final List<ScheduledTask> tasks,
                final long sessionStart, final JobNodeKey key)
        {
            myJob = job;
            myNodeId = nodeId;
            myTasks = tasks;
            mySessionStart = sessionStart;
            myKey = key;
        }

        int run()
        {
            currentExecutingJobs.put(myKey, myJob);
            try (SessionLockPool lockPool = new SessionLockPool(myLockFactory, myRepairLockFactory, myNodeId))
            {
                return executeTasks(lockPool);
            }
            finally
            {
                currentExecutingJobs.remove(myKey);
                myInFlightUnits.remove(myKey);
            }
        }

        private int executeTasks(final SessionLockPool lockPool)
        {
            int tasksExecuted = 0;
            int index = 0;
            AtomicInteger failureCounter = myConsecutiveFailures.computeIfAbsent(myKey, k -> new AtomicInteger(0));
            for (ScheduledTask task : myTasks)
            {
                if ((System.currentTimeMillis() - mySessionStart) >= mySessionWindowInMs)
                {
                    break;
                }
                index++;
                try
                {
                    lockPool.acquireForTask(task);
                    boolean successful = runTask(task, index);
                    synchronized (myJob)
                    {
                        myJob.postExecute(successful, task);
                    }
                    if (successful)
                    {
                        tasksExecuted++;
                        failureCounter.set(0);
                    }
                    else if (handleUnsuccessfulTask(failureCounter))
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    handleLockFailure(e);
                }
            }
            if (tasksExecuted > 0)
            {
                synchronized (myJob)
                {
                    myJob.refreshState();
                }
            }
            return tasksExecuted;
        }

        private boolean handleUnsuccessfulTask(final AtomicInteger failureCounter)
        {
            long rejectDelay = validateJob(myJob, nodeFor(myNodeId));
            if (rejectDelay != -1L)
            {
                myJob.setRunnableIn(rejectDelay);
                failureCounter.set(0);
                return true;
            }
            int failures = failureCounter.incrementAndGet();
            if (failures >= MAX_CONSECUTIVE_TASK_FAILURES)
            {
                LOG.error("Unified job {} on node {} failed {} consecutive tasks, marking FAILED",
                        myJob, myNodeId, failures);
                myJob.markFailed();
                return true;
            }
            return false;
        }

        private void handleLockFailure(final Exception e)
        {
            if (e instanceof RuntimeMBeanException || e instanceof LockException)
            {
                LOG.debug("Lock contention for unified job {} in node {}: {}", myJob, myNodeId, e.getMessage());
            }
            else
            {
                LOG.warn("Unable to run unified task for job {} in node {}", myJob, myNodeId, e);
            }
            long backoff = ThreadLocalRandom.current().nextLong(
                    LOCK_CONTENTION_BACKOFF_MIN_MS, LOCK_CONTENTION_BACKOFF_MAX_MS);
            myContentionBackoff.put(myKey, System.currentTimeMillis() + backoff);
        }

        private boolean runTask(final ScheduledTask task, final int index)
        {
            try
            {
                LOG.debug("Running unified task: {} ({}), for node {}", task, index, myNodeId);
                return task.execute(myNodeId);
            }
            catch (Exception e)
            {
                LOG.warn("Unable to run unified task: {} in node: {}", task, myNodeId, e);
            }
            return false;
        }
    }

    /**
     * Create a builder.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder for {@link UnifiedScheduleManager}.
     */
    public static class Builder
    {
        private static final long DEFAULT_SESSION_WINDOW_MS = TimeUnit.MINUTES.toMillis(5);
        private CASLockFactory myLockFactory;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;
        private long mySessionWindowInMs = DEFAULT_SESSION_WINDOW_MS;
        private long myCooldownInMs = 0;
        private DistributedNativeConnectionProvider myNativeConnectionProvider;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Set the run interval.
         *
         * @param runInterval the interval.
         * @param timeUnit the unit.
         * @return this builder.
         */
        public final Builder withRunInterval(final long runInterval, final TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }

        /**
         * Set the session window.
         *
         * @param sessionWindow the window.
         * @param timeUnit the unit.
         * @return this builder.
         */
        public final Builder withSessionWindow(final long sessionWindow, final TimeUnit timeUnit)
        {
            mySessionWindowInMs = timeUnit.toMillis(sessionWindow);
            return this;
        }

        /**
         * Set the cooldown.
         *
         * @param cooldown the cooldown.
         * @param timeUnit the unit.
         * @return this builder.
         */
        public final Builder withCooldown(final long cooldown, final TimeUnit timeUnit)
        {
            myCooldownInMs = timeUnit.toMillis(cooldown);
            return this;
        }

        /**
         * Set the CAS lock factory.
         *
         * @param lockFactory the lock factory.
         * @return this builder.
         */
        public final Builder withLockFactory(final CASLockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        /**
         * Set the native connection provider.
         *
         * @param nativeConnectionProvider the provider.
         * @return this builder.
         */
        public Builder withNativeConnectionProvider(final DistributedNativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnectionProvider = nativeConnectionProvider;
            return this;
        }

        /**
         * Build.
         *
         * @return UnifiedScheduleManager
         */
        public final UnifiedScheduleManager build()
        {
            return new UnifiedScheduleManager(this);
        }
    }
}
