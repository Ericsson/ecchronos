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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.RuntimeMBeanException;

/**
 * ScheduleManager handles the run scheduler and update scheduler.
 */
@SuppressWarnings("PMD.GodClass")
public final class ScheduleManagerImpl implements ScheduleManager, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerImpl.class);

    static final long DEFAULT_RUN_DELAY_IN_MS = TimeUnit.SECONDS.toMillis(30);

    private final Map<UUID, ScheduledJobQueue> myQueue = new ConcurrentHashMap<>();
    private final Collection<UUID> myNodeIDList;
    private final Map<UUID, ScheduledJob> currentExecutingJobs = new ConcurrentHashMap<>();
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();
    private final Set<UUID> myRegisteredNodes = Sets.newConcurrentHashSet();
    private final CASLockFactory myLockFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private final BlockingQueue<UUID> myWorkQueue = new LinkedBlockingQueue<>();
    private final ExecutorService myWorkerPool;
    private final ScheduledExecutorService myProducer;
    private ScheduledFuture<?> myProducerFuture;
    private final long myRunIntervalInMs;

    private ScheduleManagerImpl(final Builder builder)
    {
        myNodeIDList = builder.myNodeIDList;
        myNativeConnectionProvider = builder.myNativeConnectionProvider;
        myWorkerPool = Executors.newFixedThreadPool(
                myNodeIDList.size(),
                new ThreadFactoryBuilder().setNameFormat("TaskExecutor-%d").build());
        myProducer = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("TaskProducer").setDaemon(true).build());
        myLockFactory = builder.myLockFactory;
        myRunIntervalInMs = builder.myRunIntervalInMs;
    }

    /**
     * Create workers for each of the nodes in the nodeIDList and start the producer.
     * @param nodeIDList
     */
    @Override
    public void createScheduleFutureForNodeIDList(final Collection<UUID> nodeIDList)
    {
        LOG.debug("Total nodes found: {}", nodeIDList.size());
        nodeIDList.forEach(this::registerNode);
        startProducer();
        startWorkers();
    }

    /**
     * Register a new node for scheduling.
     * @param nodeID
     */
    @Override
    public void createScheduleFutureForNode(final UUID nodeID)
    {
        registerNode(nodeID);
    }

    private void registerNode(final UUID nodeID)
    {
        if (myRegisteredNodes.add(nodeID))
        {
            LOG.debug("Node {} registered for scheduling", nodeID);
        }
    }

    private void startProducer()
    {
        if (myProducerFuture == null)
        {
            myProducerFuture = myProducer.scheduleAtFixedRate(
                    this::produceWork, 0, myRunIntervalInMs, TimeUnit.MILLISECONDS);
        }
    }

    private void startWorkers()
    {
        for (int i = 0; i < myRegisteredNodes.size(); i++)
        {
            myWorkerPool.submit(this::workerLoop);
        }
    }

    private void produceWork()
    {
        for (UUID nodeID : myRegisteredNodes)
        {
            myWorkQueue.offer(nodeID);
        }
    }

    private void workerLoop()
    {
        while (!Thread.currentThread().isInterrupted())
        {
            try
            {
                UUID nodeID = myWorkQueue.take();
                runForNode(nodeID);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e)
            {
                LOG.error("Exception in worker loop", e);
            }
        }
    }
    @Override
    public String getCurrentJobStatus()
    {
        return currentExecutingJobs.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().getJobId())
                .collect(Collectors.joining("\n"));
    }

    /**
     * Adds a run policy to the collection of run policies.
     *
     * @param runPolicy the {@link RunPolicy} to be added. Must not be {@code null}.
     * @return {@code true} if the run policy was added successfully; {@code false} if it was already present.
     */
    public boolean addRunPolicy(final RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} added", runPolicy);
        return myRunPolicies.add(runPolicy);
    }

    /**
     * Removes a run policy from the collection of run policies.
     *
     * @param runPolicy the {@link RunPolicy} to be removed. Must not be {@code null}.
     * @return {@code true} if the run policy was successfully removed; {@code false} if it was not present.
     */

    public boolean removeRunPolicy(final RunPolicy runPolicy)
    {
        LOG.debug("Run policy {} removed", runPolicy);
        return myRunPolicies.remove(runPolicy);
    }

    @Override
    public void schedule(
            final UUID nodeID,
            final ScheduledJob job)
    {
        ScheduledJobQueue queue = myQueue.get(nodeID);
        if (queue == null)
        {
            myQueue.put(nodeID, new ScheduledJobQueue(new DefaultJobComparator()));
        }
        myQueue.get(nodeID).add(job);
    }

    @Override
    public void deschedule(final UUID nodeID, final ScheduledJob job)
    {
        myQueue.get(nodeID).remove(job);
    }

    @Override
    public void close()
    {
        if (myProducerFuture != null)
        {
            myProducerFuture.cancel(false);
        }
        myProducer.shutdown();
        myWorkerPool.shutdownNow();
        myRunPolicies.clear();
    }

    /**
     * Made available for testing.
     *
     * @param nodeID the node id to run jobs.
     */
    @VisibleForTesting
    public void run(final UUID nodeID)
    {
        runForNode(nodeID);
    }

    /**
     * Made available for testing.
     *
     * @param nodeID the node id to get queue size.
     * @return int Queue size.
     */
    @VisibleForTesting
    public int getQueueSize(final UUID nodeID)
    {
        return myQueue.get(nodeID).size();
    }

    private Long validateJob(final ScheduledJob job, final Node node)
    {
        for (RunPolicy runPolicy : myRunPolicies)
        {
            long nextRun = runPolicy.validate(job, node);
            if (nextRun != -1L)
            {
                LOG.debug("Job {} rejected for {} ms by {}", job, nextRun, runPolicy);
                return nextRun;
            }
        }

        return -1L;
    }


    private void runForNode(final UUID nodeID)
    {
        try
        {
            LOG.debug("Running for Node {}", nodeID);
            ScheduledJobQueue queue = myQueue.get(nodeID);
            if (queue != null)
            {
                boolean didWork = tryRunNext(nodeID, queue);
                if (didWork)
                {
                    myWorkQueue.offer(nodeID);
                }
            }
            else
            {
                LOG.info("There is no ScheduledJob for this node {} to run", nodeID);
            }
        }
        catch (Exception e)
        {
            LOG.error("Exception while running job in node {}", nodeID, e);
        }
    }

    private boolean tryRunNext(final UUID nodeID, final ScheduledJobQueue queue)
    {
        Node node = myNativeConnectionProvider.getNodes().get(nodeID);
        LOG.debug("Looking for Job for Node {}", nodeID);
        boolean jobRan = false;
        for (ScheduledJob next : queue)
        {
            if (validate(next, node))
            {
                currentExecutingJobs.put(nodeID, next);
                if (tryRunTasks(nodeID, node, next))
                {
                    jobRan = true;
                    break;
                }
            }
        }
        currentExecutingJobs.remove(nodeID);
        return jobRan;
    }

    private boolean validate(final ScheduledJob job, final Node node)
    {
        LOG.trace("Validating job {}", job);
        long nextRun = validateJob(job, node);
        if (nextRun != -1L)
        {
            job.setRunnableIn(nextRun);
            return false;
        }
        return true;
    }

    private boolean tryRunTasks(final UUID nodeID, final Node node, final ScheduledJob next)
    {
        boolean hasRun = false;
        for (ScheduledTask task : next)
        {
            if (!validate(next, node))
            {
                LOG.debug("Job {} was stopped, will continue later", next);
                break;
            }
            hasRun |= tryRunTask(nodeID, next, task);
        }
        return hasRun;
    }

    private boolean tryRunTask(final UUID nodeID, final ScheduledJob job, final ScheduledTask task)
    {
        LOG.debug("Trying to run task {} in node {}", task, nodeID);
        try (LockFactory.DistributedLock lock = task.getLock(myLockFactory, nodeID))
        {
            LOG.debug("Lock acquired on node {} with lock {}", nodeID, lock);
            boolean successful = runTask(nodeID, task);
            job.postExecute(successful, task);
            return true;
        }
        catch (RuntimeMBeanException e)
        {
            if (e.getCause() instanceof IllegalStateException
                    && e.getCause().getMessage() != null
                    && e.getCause().getMessage().contains("More than one key found"))
            {
                LOG.debug("Unable to get schedule lock on task {} in node {}, this is probably due to "
                        + "a connection to a version of the Jolokia Agent 2.3.0 or older", task, nodeID, e);
            }
            else
            {
                LOG.warn("Unable to get schedule lock on task {} in node {}", task, nodeID, e);
            }
            return false;
        }
        catch (Exception e)
        {
            if (e.getCause() != null)
            {
                LOG.warn("Unable to get schedule lock on task {} in node {}", task, nodeID, e);
            }
            return false;
        }
    }

    private boolean runTask(final UUID nodeID, final ScheduledTask task)
    {
        try
        {
            LOG.info("Running task: {}, for node {}", task, nodeID);
            return task.execute(nodeID);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to run task: {} in node: {}", task, nodeID, e);
        }
        return false;
    }

    /**
     * Create an instance of Builder to construct ScheduleManagerImpl.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder class to construct ScheduleManagerImpl.
     */
    public static class Builder
    {
        private Collection<UUID> myNodeIDList;
        private CASLockFactory myLockFactory;
        private long myRunIntervalInMs = DEFAULT_RUN_DELAY_IN_MS;
        private DistributedNativeConnectionProvider myNativeConnectionProvider;

        /**
         * Build SchedulerManager with run interval.
         *
         * @param runInterval the interval to run a repair task
         * @param timeUnit the TimeUnit to specify the interval
         * @return Builder with run interval
         */
        public final Builder withRunInterval(final long runInterval, final TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }

        /**
         * Build SchedulerManager with run interval.
         *
         * @param nodeIDList the interval to run a repair task
         * @return Builder with nodes list
         */
        public Builder withNodeIDList(final Collection<UUID> nodeIDList)
        {
            myNodeIDList = nodeIDList;
            return this;
        }

        /**
         * Sets the {@link CASLockFactory}.
         *
         * @param lockFactory The {@link CASLockFactory} to be used.
         *                    Must not be {@code null}.
         * @return The current {@code Builder} instance, allowing for method chaining.
         */
        public final Builder withLockFactory(final CASLockFactory lockFactory)
        {
            myLockFactory = lockFactory;
            return this;
        }

        /**
         * Build SchedulerManager with native connection provider.
         *
         * @param nativeConnectionProvider the native connection provider
         * @return Builder with native connection provider
         */
        public Builder withNativeConnectionProvider(final DistributedNativeConnectionProvider nativeConnectionProvider)
        {
            myNativeConnectionProvider = nativeConnectionProvider;
            return this;
        }

        /**
         * Build SchedulerManager with the provided configuration.
         *
         * @return ScheduleManagerImpl with provided configuration.
         */
        public final ScheduleManagerImpl build()
        {
            return new ScheduleManagerImpl(this);
        }
    }
}

