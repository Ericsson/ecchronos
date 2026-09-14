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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles JMX notifications during a repair task, parsing progress messages
 * and signaling completion or failure.
 */
public final class RepairNotificationHandler implements NotificationListener
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairNotificationHandler.class);
    private static final Pattern RANGE_PATTERN = Pattern.compile("\\((-?[0-9]+),(-?[0-9]+)\\]");

    private final CountDownLatch myLatch = new CountDownLatch(1);
    private final List<Notification> myPendingNotifications = Collections.synchronizedList(new ArrayList<>());
    private final RangeFinishedCallback myRangeCallback;
    private final HangPreventionCallback myHangPreventionCallback;

    private volatile ScheduledJobException myLastError;
    private volatile boolean hasLostNotification = false;
    private volatile int myCommand;
    private volatile boolean myCommandReady = false;

    /**
     * Callback invoked when a token range finishes repair.
     */
    @FunctionalInterface
    public interface RangeFinishedCallback
    {
        /**
         * Called when a token range repair finishes.
         *
         * @param range the token range that finished.
         * @param status the repair status of the range.
         */
        void onRangeFinished(LongTokenRange range, RepairStatus status);
    }

    /**
     * Callback invoked when hang prevention should be rescheduled.
     */
    @FunctionalInterface
    public interface HangPreventionCallback
    {
        /**
         * Reschedule the hang prevention check.
         */
        void rescheduleHangPrevention();
    }

    /**
     * Constructs a RepairNotificationHandler with the given callbacks.
     *
     * @param rangeCallback the callback invoked when a range finishes repair.
     * @param hangPreventionCallback the callback to reschedule hang prevention.
     */
    public RepairNotificationHandler(final RangeFinishedCallback rangeCallback,
            final HangPreventionCallback hangPreventionCallback)
    {
        myRangeCallback = rangeCallback;
        myHangPreventionCallback = hangPreventionCallback;
    }

    /**
     * Prepare for a new repair command. Must be called before repairAsync.
     */
    public void prepareForCommand()
    {
        myCommandReady = false;
        myPendingNotifications.clear();
    }

    /**
     * Set the command ID once repairAsync returns. Processes any pending notifications.
     *
     * @param command the repair command ID
     */
    public void setCommand(final int command)
    {
        myCommand = command;
        myCommandReady = true;
        if (!myPendingNotifications.isEmpty())
        {
            LOG.debug("Processing {} pending notifications", myPendingNotifications.size());
            for (Notification pending : new ArrayList<>(myPendingNotifications))
            {
                handleNotification(pending, null);
            }
            myPendingNotifications.clear();
        }
    }

    /**
     * Wait for the repair to complete.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void await() throws InterruptedException
    {
        myLatch.await();
    }

    /**
     * Gets the last error encountered during repair.
     *
     * @return the last error encountered, or null if none
     */
    public ScheduledJobException getLastError()
    {
        return myLastError;
    }

    /**
     * Checks if a JMX notification was lost.
     *
     * @return true if a JMX notification was lost
     */
    public boolean hasLostNotification()
    {
        return hasLostNotification;
    }

    /**
     * Gets the repair command ID.
     *
     * @return the repair command ID
     */
    public int getCommand()
    {
        return myCommand;
    }

    /**
     * Signal completion (e.g., from hang monitor).
     */
    public void countDown()
    {
        myLatch.countDown();
    }

    /**
     * Set an error (e.g., from hang monitor).
     *
     * @param error the error to record
     */
    public void setError(final ScheduledJobException error)
    {
        myLastError = error;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleNotification(final Notification notification, final Object handback)
    {
        LOG.debug("Notification {}", notification.toString());
        switch (notification.getType())
        {
        case "progress":
            myHangPreventionCallback.rescheduleHangPrevention();
            String tag = (String) notification.getSource();
            if (!myCommandReady)
            {
                LOG.debug("Notification arrived before command ID is ready, storing as pending: {}", tag);
                myPendingNotifications.add(notification);
                break;
            }
            if (tag.equals("repair:" + myCommand))
            {
                Map<String, Integer> progress = (Map<String, Integer>) notification.getUserData();
                String message = notification.getMessage();
                RepairTask.ProgressEventType type = RepairTask.ProgressEventType.values()[progress.get("type")];
                LOG.debug("Notification Type {}", type.toString());
                progress(type, message);
            }
            break;

        case JMXConnectionNotification.NOTIFS_LOST:
            hasLostNotification = true;
            break;

        case JMXConnectionNotification.FAILED:
        case JMXConnectionNotification.CLOSED:
            String errorMessage = String.format("Unable to repair, error: %s", notification.getType());
            LOG.error(errorMessage);
            myLastError = new ScheduledJobException(errorMessage);
            myLatch.countDown();
            break;
        default:
            LOG.debug("Unknown JMXConnectionNotification type: {}", notification.getType());
            break;
        }
    }

    @VisibleForTesting
    void progress(final RepairTask.ProgressEventType type, final String message)
    {
        if (type == RepairTask.ProgressEventType.PROGRESS || type == RepairTask.ProgressEventType.ERROR)
        {
            boolean parsedRange = parseRangeResult(message);

            // A terminal ERROR event that does not carry a parseable per-range result is a session-level
            // failure (e.g. an incremental prepare-phase abort where the SSTables could not be acquired).
            // Without this, the outcome would be inferred purely from message substrings and the task would
            // default to SUCCESS even though no data was repaired.
            if (type == RepairTask.ProgressEventType.ERROR && !parsedRange)
            {
                recordFailure(message);
            }
        }
        else if (type == RepairTask.ProgressEventType.ABORT)
        {
            // An aborted repair never repaired the owned ranges; treat it as a failure regardless of message text.
            recordFailure(message);
        }

        if (type == RepairTask.ProgressEventType.COMPLETE)
        {
            LOG.debug("Progress message set to complete, latch counted down: {}", message);
            myLatch.countDown();
        }
    }

    /**
     * Parse per-range results from a progress message and forward them to the range callback.
     *
     * @param message the progress notification message.
     * @return true if the message contained a recognized per-range result (finished/failed with a token range).
     */
    private boolean parseRangeResult(final String message)
    {
        if (!message.contains("finished") && !message.contains("failed"))
        {
            LOG.warn("Unknown progress message received: {}", message);
            return false;
        }
        LOG.debug("Progress message received: {}", message);
        RepairStatus repairStatus = message.contains("failed") ? RepairStatus.FAILED : RepairStatus.SUCCESS;
        boolean matchedRange = false;
        Matcher rangeMatcher = RANGE_PATTERN.matcher(message);
        while (rangeMatcher.find())
        {
            long start = Long.parseLong(rangeMatcher.group(1));
            long end = Long.parseLong(rangeMatcher.group(2));
            myRangeCallback.onRangeFinished(LongTokenRange.of(start, end), repairStatus);
            matchedRange = true;
        }
        return matchedRange;
    }

    /**
     * Record a session-level repair failure so that the awaiting task fails instead of defaulting to success.
     *
     * @param message the message describing the failure.
     */
    private void recordFailure(final String message)
    {
        String errorMessage = String.format("Repair failed: %s", message);
        LOG.warn(errorMessage);
        if (myLastError == null)
        {
            myLastError = new ScheduledJobException(errorMessage);
        }
    }
}
