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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class used to represent repair tasks. Orchestrates repair execution
 * by delegating to {@link RepairRangeTracker}, {@link RepairNotificationHandler},
 * and {@link RepairHangMonitor}.
 */
public abstract class RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairTask.class);

    private final UUID nodeID;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableReference myTableReference;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairConfiguration myRepairConfiguration;

    private final RepairRangeTracker myRangeTracker = new RepairRangeTracker();
    private final RepairNotificationHandler myNotificationHandler;
    private final RepairHangMonitor myHangMonitor;

    /**
     * Constructs a RepairTask for the specified node and table with the given repair configuration and metrics.
     *
     * @param currentNodeID the UUID of the current node where the repair task is running. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the reference to the table that is being repaired. Must not be {@code null}.
     * @param repairConfiguration the configuration specifying how the repair task should be executed. Must not be {@code null}.
     * @param tableRepairMetrics the metrics associated with table repairs for monitoring and tracking purposes. May be {@code null}.
     * @param maxWaitTime the maximum number of minutes to wait for a repair to complete before forcing termination.
     */
    protected RepairTask(
            final UUID currentNodeID,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics,
            final Integer maxWaitTime
    )
    {
        nodeID = currentNodeID;
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "Jmx proxy factory must be set");
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myTableRepairMetrics = tableRepairMetrics;

        myNotificationHandler = new RepairNotificationHandler(
                this::onRangeFinished,
                this::rescheduleHangPrevention
        );
        myHangMonitor = new RepairHangMonitor(
                myJmxProxyFactory, nodeID, myTableReference, maxWaitTime, myNotificationHandler
        );
    }

    /**
     * Execute the repair task.
     *
     * @throws ScheduledJobException
     *         Scheduled job exception if the repair fails.
     */
    public void execute() throws ScheduledJobException
    {
        long start = System.nanoTime();
        long end;
        long total;
        boolean successful = true;
        onExecute();
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            myHangMonitor.reschedule();
            repair(proxy);
            onFinish(RepairStatus.SUCCESS);
        }
        catch (Exception e)
        {
            onFinish(RepairStatus.FAILED);
            successful = false;
            throw new ScheduledJobException("Unable to repair '" + this + "'", e);
        }
        finally
        {
            myHangMonitor.cancel();
            end = System.nanoTime();
            total = end - start;
            myTableRepairMetrics.repairSession(myTableReference, total, TimeUnit.NANOSECONDS, successful);
        }
        lazySleep(total);
    }

    /**
     * Method called before the task is executed, default implementation is NOOP.
     */
    protected void onExecute()
    {
        // NOOP
    }

    private void repair(final DistributedJmxProxy proxy) throws ScheduledJobException
    {
        LOG.debug("repair starting for table {} on node {}", myTableReference, nodeID);
        if (!proxy.addStorageServiceListener(nodeID, myNotificationHandler))
        {
            String msg = String.format("Repair of %s has no jmx connection", myTableReference);
            LOG.warn(msg);
            throw new ScheduledJobException(msg);
        }
        try
        {
            myNotificationHandler.prepareForCommand();
            int command = proxy.repairAsync(nodeID, myTableReference.getKeyspace(), getOptions());
            if (command > 0)
            {
                myNotificationHandler.setCommand(command);
                awaitAndVerifyRepair(proxy, command);
            }
        }
        finally
        {
            proxy.removeStorageServiceListener(nodeID, myNotificationHandler);
        }
    }

    private void awaitAndVerifyRepair(final DistributedJmxProxy proxy, final int command)
            throws ScheduledJobException
    {
        try
        {
            LOG.debug("waiting for latch for table {} on node {}", myTableReference, nodeID);
            myNotificationHandler.await();
            LOG.debug("finished waiting for latch for table {} on node {}", myTableReference, nodeID);
            verifyRepair(proxy);
            if (myNotificationHandler.getLastError() != null)
            {
                throw myNotificationHandler.getLastError();
            }
            if (myNotificationHandler.hasLostNotification())
            {
                String msg = String.format("Repair-%d of %s had lost notifications", command, myTableReference);
                LOG.warn(msg);
                throw new ScheduledJobException(msg);
            }
            LOG.debug("{} completed successfully on node {}", this, nodeID);
        }
        catch (InterruptedException e)
        {
            String msg = this + " was interrupted";
            LOG.warn(msg, e);
            Thread.currentThread().interrupt();
            throw new ScheduledJobException(msg, e);
        }
    }

    private void rescheduleHangPrevention()
    {
        myHangMonitor.reschedule();
    }

    /**
     * Method used to construct options for the repair.
     *
     * @return Options
     */
    protected abstract Map<String, String> getOptions();

    /**
     * Method is called once a repair is completed.
     *
     * @param proxy
     *         The jmx proxy
     * @throws ScheduledJobException
     *         In case when repair is deemed as failed.
     */
    protected void verifyRepair(final DistributedJmxProxy proxy) throws ScheduledJobException
    {
        if (myRangeTracker.hasFailedRanges())
        {
            proxy.forceTerminateAllRepairSessions();
            throw new ScheduledJobException("Repair has failed ranges '" + myRangeTracker.getFailedRanges() + "'");
        }
    }

    /**
     * Method called when the task is finished.
     *
     * @param repairStatus
     *         The status of the finished task.
     */
    protected abstract void onFinish(RepairStatus repairStatus);

    private void lazySleep(final long executionInNanos) throws ScheduledJobException
    {
        if (myRepairConfiguration.getRepairUnwindRatio() != RepairConfiguration.NO_UNWIND)
        {
            double sleepDurationInNanos = executionInNanos * myRepairConfiguration.getRepairUnwindRatio();
            long sleepDurationInMs = TimeUnit.NANOSECONDS.toMillis((long) sleepDurationInNanos);
            sleepDurationInMs = Math.max(sleepDurationInMs, 1);
            try
            {
                Thread.sleep(sleepDurationInMs);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new ScheduledJobException(e);
            }
        }
    }

    /**
     * Clean up the repair task.
     */
    public void cleanup()
    {
        myHangMonitor.cancel();
    }

    /**
     * Method called once a range is finished. In case of multiple ranges being repaired this will be
     * called once per range. If this method is overridden make sure to call the super method.
     *
     * @param range The range
     * @param repairStatus The status of the range
     */
    protected void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        myRangeTracker.onRangeFinished(range, repairStatus);
    }

    /**
     * Enum used to provide Event Progress for RepairTask.
     */
    public enum ProgressEventType
    {
        /**
         * Fired first when progress starts. Happens only once.
         */
        START,

        /**
         * Fire when progress happens. This can be zero or more time after START.
         */
        PROGRESS,

        /**
         * When observing process completes with error, this is sent once before COMPLETE.
         */
        ERROR,

        /**
         * When observing process is aborted by user, this is sent once before COMPLETE.
         */
        ABORT,

        /**
         * When observing process completes successfully, this is sent once before COMPLETE.
         */
        SUCCESS,

        /**
         * Fire when progress complete. This is fired once, after ERROR/ABORT/SUCCESS is fired. After this, no more
         * ProgressEvent should be fired for the same event.
         */
        COMPLETE,

        /**
         * Used when sending message without progress.
         */
        NOTIFICATION
    }

    /**
     * Returns the set of token ranges that have failed during the repair task.
     *
     * @return a set of {@link LongTokenRange} representing the failed token ranges.
     */
    @VisibleForTesting
    protected final Set<LongTokenRange> getFailedRanges()
    {
        return myRangeTracker.getFailedRanges();
    }

    @VisibleForTesting
    protected final Set<LongTokenRange> getSuccessfulRanges()
    {
        return myRangeTracker.getSuccessfulRanges();
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get the repair configuration.
     *
     * @return RepairConfiguration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }
}
