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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

/**
 * A task that is run to repair a specific keyspace and table using the options from {@link RepairOptions}.
 * <p>
 * If the repair failed the {@link #getUnknownRanges()} can be used to retrieve the ranges that have an unknown status during the repair.
 */
public class RepairTask implements NotificationListener
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairTask.class);

    private static final Pattern REPAIR_PATTERN = Pattern.compile("Repair session [0-9a-zA-Z-]+ for range \\[\\(([-]?[0-9]+),([-]?[0-9]+)(\\]){2} finished");

    private static final long HANG_PREVENT_TIME_IN_MINUTES = 30;

    private final ScheduledExecutorService myExecutor = Executors.newScheduledThreadPool(1);

    private final Set<LongTokenRange> completedRanges = Collections.synchronizedSet(new HashSet<>());
    private final CountDownLatch myLatch = new CountDownLatch(1);

    private final Set<LongTokenRange> myTokenRanges;
    private final Set<Host> myReplicas;
    private final boolean vnodeRepair;
    private final JmxProxyFactory myJmxProxyFactory;
    private final TableReference myTableReference;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairConfiguration myRepairConfiguration;

    private volatile boolean hasLostNotification = false;
    private volatile ScheduledJobException myLastError = null;
    private volatile Collection<LongTokenRange> myUnknownRanges = null;

    private volatile ScheduledFuture<?> myHangPreventFuture;
    private volatile int myCommand;

    RepairTask(Builder builder)
    {
        myJmxProxyFactory = builder.jmxProxyFactory;
        myTableReference = builder.tableReference;
        myTokenRanges = builder.tokenRanges;
        myReplicas = builder.replicas;
        vnodeRepair = builder.vnodeRepair;
        myTableRepairMetrics = builder.tableRepairMetrics;
        myRepairConfiguration = builder.repairConfiguration;
    }

    public void execute() throws ScheduledJobException
    {
        long start = System.nanoTime();
        long end;
        long executionNanos;
        boolean successful = true;

        try (JmxProxy proxy = myJmxProxyFactory.connect())
        {
            rescheduleHangPrevention();
            repair(proxy);
        }
        catch (Exception e)
        {
            successful = false;
            String msg = "Unable to repair " + this;
            LOG.warn(msg);
            throw new ScheduledJobException(msg, e);
        }
        finally
        {
            if (myHangPreventFuture != null)
            {
                myHangPreventFuture.cancel(false);
                myHangPreventFuture = null;
            }
            end = System.nanoTime();
            executionNanos = end - start;

            myTableRepairMetrics.repairTiming(myTableReference, executionNanos, TimeUnit.NANOSECONDS, successful);
        }

        lazySleep(executionNanos);
    }

    private void lazySleep(long executionNanos) throws ScheduledJobException
    {
        if (myRepairConfiguration.getUnwindRatio() != RepairConfiguration.NO_UNWIND)
        {
            double sleepDurationNanos = executionNanos * myRepairConfiguration.getUnwindRatio();
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

    public void cleanup()
    {
        myExecutor.shutdown();
    }

    /**
     * Get the ranges that failed during this repair.
     *
     * @return The ranges that failed or null if none failed.
     */
    public Collection<LongTokenRange> getUnknownRanges()
    {
        return myUnknownRanges == null ? null : new HashSet<>(myUnknownRanges);
    }

    @SuppressWarnings ("unchecked")
    @Override
    public void handleNotification(Notification notification, Object handback)
    {
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
                    int progressCount = progress.get("progressCount");
                    int total = progress.get("total");

                    this.progress(type, progressCount, total, message);
                }
                break;

            case JMXConnectionNotification.NOTIFS_LOST:
                hasLostNotification = true;
                break;

            case JMXConnectionNotification.FAILED: // NOPMD
            case JMXConnectionNotification.CLOSED:
                handleConnectionFailed();
                break;
            default:
                LOG.debug("Unknown JMXConnectionNotification type: {}", notification.getType());
                break;
        }
    }

    @Override
    public String toString()
    {
        return String.format("Repair of %s", myTableReference);
    }

    private Map<String, String> getOptions()
    {
        Map<String, String> options = new HashMap<>();

        options.put(RepairOptions.PARALLELISM_KEY, myRepairConfiguration.getRepairParallelism().getName());
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, myTableReference.getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(myRepairConfiguration.getRepairType().equals(RepairOptions.RepairType.INCREMENTAL)));

        if (vnodeRepair)
        {
            StringBuilder sb = new StringBuilder();

            for (LongTokenRange range : myTokenRanges)
            {
                sb.append(range.start).append(':').append(range.end).append(',');
            }

            options.put(RepairOptions.RANGES_KEY, sb.toString());
        }

        if (myReplicas != null)
        {
            StringBuilder sb = new StringBuilder();

            for (Host host : myReplicas)
            {
                sb.append(host.getBroadcastAddress().getHostAddress()).append(',');
            }

            options.put(RepairOptions.HOSTS_KEY, sb.toString());
        }

        return options;
    }

    private void repair(JmxProxy proxy) throws ScheduledJobException
    {
        proxy.addStorageServiceListener(this);
        myCommand = proxy.repairAsync(myTableReference.getKeyspace(), getOptions());

        if (myCommand > 0)
        {
            try
            {
                myLatch.await();

                proxy.removeStorageServiceListener(this);

                if (!validateRepairedRanges())
                {
                    proxy.forceTerminateAllRepairSessions();
                    String msg = String.format("Unknown status of some ranges for %s", this);
                    LOG.warn(msg);
                    throw new ScheduledJobException(msg);
                }

                if (myLastError != null)
                {
                    throw myLastError;
                }

                if (hasLostNotification)
                {
                    String msg = String.format("Repair of %s had lost notifications", myTableReference);
                    LOG.warn(msg);
                    throw new ScheduledJobException(msg);
                }

                LOG.debug("{} - {} completed successfully", this, completedRanges);
            }
            catch (InterruptedException e)
            {
                LOG.warn("{} was interrupted", this, e);
                Thread.currentThread().interrupt();
                throw new ScheduledJobException(e);
            }
        }
    }

    private boolean validateRepairedRanges()
    {
        Set<LongTokenRange> unknownRanges = Sets.difference(myTokenRanges, completedRanges);

        if (!unknownRanges.isEmpty())
        {
            LOG.debug("Failed ranges: {}", unknownRanges);
            LOG.debug("Completed ranges: {}", completedRanges);
            myUnknownRanges = Collections.unmodifiableSet(unknownRanges);
            return false;
        }

        return true;
    }

    private void handleConnectionFailed()
    {
        myLastError = new ScheduledJobException(String.format("Unable to repair %s", myTableReference));
        myLatch.countDown();
    }

    @VisibleForTesting
    void progress(ProgressEventType type, int progressCount, int total, String message)
    {
        if (type == ProgressEventType.PROGRESS)
        {
            Matcher matcher = REPAIR_PATTERN.matcher(message);

            if (matcher.matches())
            {
                long start = Long.parseLong(matcher.group(1));
                long end = Long.parseLong(matcher.group(2));

                completedRanges.add(new LongTokenRange(start, end));
            }
            else
            {
                LOG.warn("{} - Unknown progress message received: {}", this, message);
            }

            int currentProgress = (int) calculateProgress(progressCount, total);

            if (LOG.isTraceEnabled())
            {
                LOG.trace("{} (progress: {}%)", message, currentProgress);
            }
        }

        if (type == ProgressEventType.COMPLETE)
        {
            myLatch.countDown();
        }
    }

    private double calculateProgress(int progressCount, int total)
    {
        if (total == 0)
        {
            return 0.0d;
        }

        return (progressCount * 100.0d) / total;
    }

    private void rescheduleHangPrevention()
    {
        if (myHangPreventFuture != null)
        {
            myHangPreventFuture.cancel(false);
        }
        myHangPreventFuture = myExecutor.schedule(new HangPreventingTask(), HANG_PREVENT_TIME_IN_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * A builder class for repair tasks.
     */
    public static class Builder
    {
        private boolean vnodeRepair = true;

        private JmxProxyFactory jmxProxyFactory;
        private TableReference tableReference;
        private Set<LongTokenRange> tokenRanges;
        private Set<Host> replicas;
        private TableRepairMetrics tableRepairMetrics;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;

        public Builder withVnodeRepair(boolean vnodeRepair)
        {
            this.vnodeRepair = vnodeRepair;
            return this;
        }

        public Builder withJMXProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            this.jmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableReference(TableReference tableReference)
        {
            this.tableReference = tableReference;
            return this;
        }

        public Builder withTokenRanges(Collection<LongTokenRange> tokenRanges)
        {
            this.tokenRanges = new HashSet<>(tokenRanges);
            return this;
        }

        public Builder withReplicas(Collection<Host> replicas)
        {
            this.replicas = new HashSet<>(replicas);
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public RepairTask build()
        {
            if (tableRepairMetrics == null)
            {
                throw new IllegalArgumentException("Metric interface not set");
            }

            return new RepairTask(this);
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
         * Fire when progress complete. This is fired once, after ERROR/ABORT/SUCCESS is fired. After this, no more ProgressEvent should be fired for
         * the same event.
         */
        COMPLETE,

        /**
         * Used when sending message without progress.
         */
        NOTIFICATION
    }

    private class HangPreventingTask implements Runnable
    {

        @Override
        public void run()
        {
            try (JmxProxy proxy = myJmxProxyFactory.connect())
            {
                proxy.forceTerminateAllRepairSessions();
            }
            catch (IOException e)
            {
                LOG.error("Unable to prevent hanging repair task: {}", this, e);
            }
            myLatch.countDown();
        }

    }

    @VisibleForTesting
    Set<LongTokenRange> getTokenRanges()
    {
        return Sets.newHashSet(myTokenRanges);
    }

    @VisibleForTesting
    Collection<LongTokenRange> getCompletedRanges()
    {
        return Sets.newHashSet(completedRanges);
    }

    @VisibleForTesting
    Set<Host> getReplicas()
    {
        if (myReplicas == null)
        {
            return Collections.emptySet();
        }
        return Sets.newHashSet(myReplicas);
    }

    @VisibleForTesting
    boolean isVnodeRepair()
    {
        return vnodeRepair;
    }

    @VisibleForTesting
    TableReference getTableReference()
    {
        return myTableReference;
    }

    @VisibleForTesting
    RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }
}
