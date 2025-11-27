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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract Class used to represent repair tasks.
 */
public abstract class RepairTask implements NotificationListener
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairTask.class);
    private static final Pattern RANGE_PATTERN = Pattern.compile("\\((-?[0-9]+),(-?[0-9]+)\\]");
    private static final int HEALTH_CHECK_INTERVAL_IN_MINUTES = 3;

    private final UUID nodeID;
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("HangPreventingTask-%d").build());
    private final CountDownLatch myLatch = new CountDownLatch(1);
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableReference myTableReference;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairConfiguration myRepairConfiguration;
    private volatile ScheduledFuture<?> myHangPreventFuture;
    private volatile ScheduledJobException myLastError;
    private volatile boolean hasLostNotification = false;
    private volatile int myCommand;
    private volatile Set<LongTokenRange> myFailedRanges = new HashSet<>();
    private volatile Set<LongTokenRange> mySuccessfulRanges = new HashSet<>();

    /**
     * Constructs a RepairTask for the specified node and table with the given repair configuration and metrics.
     *
     * @param currentNodeID the UUID of the current node where the repair task is running. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the reference to the table that is being repaired. Must not be {@code null}.
     * @param repairConfiguration the configuration specifying how the repair task should be executed. Must not be {@code null}.
     * @param tableRepairMetrics the metrics associated with table repairs for monitoring and tracking purposes. May be {@code null}.
     */
    protected RepairTask(
            final UUID currentNodeID,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics
    )
    {
        nodeID = currentNodeID;
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "Jmx proxy factory must be set");
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myTableRepairMetrics = tableRepairMetrics;
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
            rescheduleHangPrevention();
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
            if (myHangPreventFuture != null)
            {
                myHangPreventFuture.cancel(false);
            }
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
        proxy.addStorageServiceListener(nodeID, this);
        myCommand = proxy.repairAsync(nodeID, myTableReference.getKeyspace(), getOptions());
        if (myCommand > 0)
        {
            try
            {
                LOG.debug("waiting for latch for table {} on node {}", myTableReference, nodeID);
                myLatch.await();
                LOG.debug("finished waiting for latch for table {} on node {}", myTableReference, nodeID);
                proxy.removeStorageServiceListener(nodeID, this);
                verifyRepair(proxy);
                LOG.debug("Repair verified for table {} on node {}", myTableReference, nodeID);
                if (myLastError != null)
                {
                    throw myLastError;
                }
                if (hasLostNotification)
                {
                    String msg = String.format("Repair-%d of %s had lost notifications", myCommand, myTableReference);
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
        if (!myFailedRanges.isEmpty())
        {
            proxy.forceTerminateAllRepairSessions();
            throw new ScheduledJobException("Repair has failed ranges '" + myFailedRanges + "'");
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
        myExecutor.shutdown();
    }

    /**
     * Notification handler.
     *
     * @param notification
     *         The notification.
     * @param handback
     *         The handback.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void handleNotification(final Notification notification, final Object handback)
    {
        LOG.debug("Notification {}", notification.toString());
        switch (notification.getType())
        {
        case "progress":
            rescheduleHangPrevention();
            String tag = (String) notification.getSource();
            if (tag.equals("repair:" + myCommand))
            {
                Map<String, Integer> progress = (Map<String, Integer>) notification.getUserData();

                String message = notification.getMessage();
                ProgressEventType type = ProgressEventType.values()[progress.get("type")];

                LOG.debug("Notification Type {}", type.toString());

                this.progress(type, message);
            }
            break;

        case JMXConnectionNotification.NOTIFS_LOST:
            hasLostNotification = true;
            break;

        case JMXConnectionNotification.FAILED:
        case JMXConnectionNotification.CLOSED:
            String errorMessage = String.format("Unable to repair %s, error: %s", myTableReference, notification.getType());
            LOG.error(errorMessage);
            myLastError = new ScheduledJobException(errorMessage);
            myLatch.countDown();
            break;
        default:
            LOG.debug("Unknown JMXConnectionNotification type: {}", notification.getType());
            break;
        }
    }

    private void rescheduleHangPrevention()
    {
        if (myHangPreventFuture != null)
        {
            myHangPreventFuture.cancel(false);
        }
        // Schedule the first check to happen after 10 minutes
        myHangPreventFuture = myExecutor.schedule(new HangPreventingTask(), HEALTH_CHECK_INTERVAL_IN_MINUTES,
                TimeUnit.MINUTES);
    }

    /**
     * Update progress.
     *
     * @param type
     *         Progress event type.
     * @param message
     *         The message.
     */
    @VisibleForTesting
    void progress(final ProgressEventType type, final String message)
    {
        if (type == ProgressEventType.PROGRESS || type == ProgressEventType.ERROR)
        {
            if (message.contains("finished") || message.contains("failed"))
            {
                LOG.debug("Progress message received: {}", message);
                RepairStatus repairStatus = RepairStatus.SUCCESS;
                if (message.contains("failed"))
                {
                    repairStatus = RepairStatus.FAILED;
                }
                Matcher rangeMatcher = RANGE_PATTERN.matcher(message);
                while (rangeMatcher.find())
                {
                    long start = Long.parseLong(rangeMatcher.group(1));
                    long end = Long.parseLong(rangeMatcher.group(2));

                    LongTokenRange completedRange = new LongTokenRange(start, end);
                    onRangeFinished(completedRange, repairStatus);
                }
            }
            else
            {
                LOG.warn("{} - Unknown progress message received: {}", this, message);
            }
        }
        if (type == ProgressEventType.COMPLETE)
        {
            LOG.debug("Progress message set to complete latch counted down: {}", message);
            myLatch.countDown();
            LOG.debug("Latch count now set to: {}", myLatch.getCount());
            myLatch.getCount();
        }
    }

    /**
     * Method called once a range is finished successfully. In case of multiple ranges being repaired this will be
     * called once per range. If this method is overriden make sure to call the super method.
     *
     * @param range The range
     * @param repairStatus The status of the range
     */
    protected void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        if (repairStatus.equals(RepairStatus.FAILED))
        {
            myFailedRanges.add(range);
        }
        else
        {
            mySuccessfulRanges.add(range);
        }
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

    private final class HangPreventingTask implements Runnable
    {
        private static final int MAX_CHECKS = 3;
        private static final String NORMAL_STATUS = "NORMAL";
        private int checkCount = 0;

        @Override
        public void run()
        {
            try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
            {
                if (checkCount < MAX_CHECKS)
                {
                    String nodeStatus = proxy.getNodeStatus(nodeID);
                    if (!NORMAL_STATUS.equals(nodeStatus))
                    {
                        LOG.error("Cassandra node {} is down, aborting repair task.", nodeID);
                        myLastError = new ScheduledJobException("Cassandra node " + nodeID + " is down");
                        proxy.forceTerminateAllRepairSessionsInSpecificNode(nodeID);
                        myLatch.countDown(); // Signal to abort the repair task
                    }
                    else
                    {
                        checkCount++;
                        myHangPreventFuture = myExecutor.schedule(this, HEALTH_CHECK_INTERVAL_IN_MINUTES, TimeUnit.MINUTES);
                    }
                }
                else
                {
                    // After 3 successful checks or 30 minutes if still task is running terminate all repair sessions
                    LOG.warn("Repair session terminated after 30 minutes for node {}", nodeID);
                    proxy.forceTerminateAllRepairSessionsInSpecificNode(nodeID);
                    myLatch.countDown();
                }
            }
            catch (IOException e)
            {
                LOG.error("Unable to check node status or prevent hanging repair task: {}", this, e);
            }
        }
    }

    /**
     * Returns the set of token ranges that have failed during the repair task.
     *
     * <p>This method is primarily intended for testing purposes.</p>
     *
     * @return a set of {@link LongTokenRange} representing the failed token ranges.
     */
    @VisibleForTesting
    protected final Set<LongTokenRange> getFailedRanges()
    {
        return myFailedRanges;
    }

    @VisibleForTesting
    protected final Set<LongTokenRange> getSuccessfulRanges()
    {
        return mySuccessfulRanges;
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
