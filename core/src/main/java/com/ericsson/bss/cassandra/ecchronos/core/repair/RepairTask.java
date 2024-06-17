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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

public abstract class RepairTask implements NotificationListener
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairTask.class);
    private static final Pattern RANGE_PATTERN = Pattern.compile("\\((-?[0-9]+),(-?[0-9]+)\\]");
    private static final int HEALTH_CHECK_INTERVAL = 10;
    private final ScheduledExecutorService myExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("HangPreventingTask-%d").build());
    private final CountDownLatch myLatch = new CountDownLatch(1);
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableReference myTableReference;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairConfiguration myRepairConfiguration;
    private volatile ScheduledFuture<?> myHangPreventFuture;
    private volatile ScheduledJobException myLastError;
    private volatile boolean hasLostNotification = false;
    private volatile int myCommand;
    private volatile Set<LongTokenRange> myFailedRanges = new HashSet<>();
    private volatile Set<LongTokenRange> mySuccessfulRanges = new HashSet<>();

    RepairTask(final JmxProxyFactory jmxProxyFactory, final TableReference tableReference,
            final RepairConfiguration repairConfiguration, final TableRepairMetrics tableRepairMetrics)
    {
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
        long executionNanos;
        boolean successful = true;
        onExecute();
        try (JmxProxy proxy = myJmxProxyFactory.connect())
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
            executionNanos = end - start;
            myTableRepairMetrics.repairSession(myTableReference, executionNanos, TimeUnit.NANOSECONDS, successful);
        }

        lazySleep(executionNanos);
    }

    /**
     * Method called before the task is executed, default implementation is NOOP.
     */
    protected void onExecute()
    {
        // NOOP
    }

    private void repair(final JmxProxy proxy) throws ScheduledJobException
    {
        proxy.addStorageServiceListener(this);
        myCommand = proxy.repairAsync(myTableReference.getKeyspace(), getOptions());
        if (myCommand > 0)
        {
            try
            {
                myLatch.await();
                proxy.removeStorageServiceListener(this);
                verifyRepair(proxy);
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
                LOG.info("{} completed successfully", this);
            }
            catch (InterruptedException e)
            {
                LOG.warn("{} was interrupted", this, e);
                Thread.currentThread().interrupt();
                throw new ScheduledJobException(e);
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
    protected void verifyRepair(final JmxProxy proxy) throws ScheduledJobException
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

    private void lazySleep(final long executionNanos) throws ScheduledJobException
    {
        if (myRepairConfiguration.getRepairUnwindRatio() != RepairConfiguration.NO_UNWIND)
        {
            double sleepDurationNanos = executionNanos * myRepairConfiguration.getRepairUnwindRatio();
            long sleepDurationMs = TimeUnit.NANOSECONDS.toMillis((long) sleepDurationNanos);
            sleepDurationMs = Math.max(sleepDurationMs, 1);
            try
            {
                Thread.sleep(sleepDurationMs);
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

                this.progress(type, message);
            }
            break;

        case JMXConnectionNotification.NOTIFS_LOST:
            hasLostNotification = true;
            break;

        case JMXConnectionNotification.FAILED:
        case JMXConnectionNotification.CLOSED:
            myLastError = new ScheduledJobException(
                    String.format("Unable to repair %s, error: %s", myTableReference, notification.getType()));
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
        myHangPreventFuture = myExecutor.schedule(new HangPreventingTask(), HEALTH_CHECK_INTERVAL,
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
            myLatch.countDown();
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

    private class HangPreventingTask implements Runnable
    {
        private static final int MAX_CHECKS = 3;
        private static final String NORMAL_STATUS = "NORMAL";
        private int checkCount = 0;

        @Override
        public void run()
        {
            try (JmxProxy proxy = myJmxProxyFactory.connect())
            {
                if (checkCount < MAX_CHECKS)
                {
                    String nodeStatus = proxy.getNodeStatus();
                    if (!NORMAL_STATUS.equals(nodeStatus))
                    {
                        LOG.error("Local Cassandra node is down, aborting repair task.");
                        myLastError = new ScheduledJobException("Local Cassandra node is down");
                        proxy.forceTerminateAllRepairSessions();
                        myLatch.countDown(); // Signal to abort the repair task
                    }
                    else
                    {
                        checkCount++;
                        myHangPreventFuture = myExecutor.schedule(this, HEALTH_CHECK_INTERVAL, TimeUnit.MINUTES);
                    }
                }
                else
                {
                    // After 3 successful checks or 30 minutes if still task is running terminate all repair sessions
                    proxy.forceTerminateAllRepairSessions();
                    myLatch.countDown();
                }
            }
            catch (IOException e)
            {
                LOG.error("Unable to check node status or prevent hanging repair task: {}", this, e);
            }
        }
    }


    @VisibleForTesting
    final Set<LongTokenRange> getFailedRanges()
    {
        return myFailedRanges;
    }

    @VisibleForTesting
    final Set<LongTokenRange> getSuccessfulRanges()
    {
        return mySuccessfulRanges;
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    protected TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get the repair configuration.
     *
     * @return RepairConfiguration
     */
    protected RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }
}
