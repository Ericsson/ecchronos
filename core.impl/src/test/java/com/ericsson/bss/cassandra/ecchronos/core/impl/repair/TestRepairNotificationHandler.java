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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.management.Notification;
import javax.management.remote.JMXConnectionNotification;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairNotificationHandler
{
    private static final int COMMAND = 42;

    @Mock
    private RepairNotificationHandler.HangPreventionCallback myHangPreventionCallback;

    private final List<RangeResult> myRangeResults = new ArrayList<>();
    private RepairNotificationHandler handler;

    @Before
    public void setup()
    {
        handler = new RepairNotificationHandler(
                (range, status) -> myRangeResults.add(new RangeResult(range, status)),
                myHangPreventionCallback
        );
        handler.prepareForCommand();
        handler.setCommand(COMMAND);
    }

    @Test
    public void testProgressNotificationParsesFinishedRange()
    {
        Notification notification = createProgressNotification(
                "repair:" + COMMAND,
                "Repair session 1 on range (1,100] for keyspace finished",
                RepairTask.ProgressEventType.PROGRESS.ordinal());

        handler.handleNotification(notification, null);

        assertThat(myRangeResults).hasSize(1);
        assertThat(myRangeResults.get(0).range).isEqualTo(new LongTokenRange(1, 100));
        assertThat(myRangeResults.get(0).status).isEqualTo(RepairStatus.SUCCESS);
    }

    @Test
    public void testProgressNotificationParsesFailedRange()
    {
        Notification notification = createProgressNotification(
                "repair:" + COMMAND,
                "Repair session 1 on range (-100,200] for keyspace failed",
                RepairTask.ProgressEventType.PROGRESS.ordinal());

        handler.handleNotification(notification, null);

        assertThat(myRangeResults).hasSize(1);
        assertThat(myRangeResults.get(0).range).isEqualTo(new LongTokenRange(-100, 200));
        assertThat(myRangeResults.get(0).status).isEqualTo(RepairStatus.FAILED);
    }

    @Test
    public void testCompleteEventCountsDownLatch() throws InterruptedException
    {
        Notification notification = createProgressNotification(
                "repair:" + COMMAND,
                "Repair completed successfully",
                RepairTask.ProgressEventType.COMPLETE.ordinal());

        handler.handleNotification(notification, null);

        // await should return immediately since latch was counted down
        CountDownLatch verifyLatch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try
            {
                handler.await();
                verifyLatch.countDown();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });
        t.start();
        assertThat(verifyLatch.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testConnectionClosedSetsError()
    {
        Notification notification = new Notification(
                JMXConnectionNotification.CLOSED, "source", 1L);

        handler.handleNotification(notification, null);

        assertThat(handler.getLastError()).isNotNull();
    }

    @Test
    public void testLostNotificationFlagSet()
    {
        Notification notification = new Notification(
                JMXConnectionNotification.NOTIFS_LOST, "source", 1L);

        handler.handleNotification(notification, null);

        assertThat(handler.hasLostNotification()).isTrue();
    }

    @Test
    public void testPendingNotificationsProcessedAfterCommandSet()
    {
        // Create a fresh handler without setting command
        RepairNotificationHandler freshHandler = new RepairNotificationHandler(
                (range, status) -> myRangeResults.add(new RangeResult(range, status)),
                myHangPreventionCallback
        );
        freshHandler.prepareForCommand();

        // Send notification before command is ready
        Notification notification = createProgressNotification(
                "repair:" + COMMAND,
                "Repair session 1 on range (50,150] for keyspace finished",
                RepairTask.ProgressEventType.PROGRESS.ordinal());
        freshHandler.handleNotification(notification, null);

        // Not processed yet
        assertThat(myRangeResults).isEmpty();

        // Now set command - pending should be processed
        freshHandler.setCommand(COMMAND);

        assertThat(myRangeResults).hasSize(1);
        assertThat(myRangeResults.get(0).range).isEqualTo(new LongTokenRange(50, 150));
    }

    @Test
    public void testProgressReschedulesHangPrevention()
    {
        Notification notification = createProgressNotification(
                "repair:" + COMMAND,
                "Repair session 1 on range (1,100] for keyspace finished",
                RepairTask.ProgressEventType.PROGRESS.ordinal());

        handler.handleNotification(notification, null);

        verify(myHangPreventionCallback).rescheduleHangPrevention();
    }

    @Test
    public void testNotificationForDifferentCommandIgnored()
    {
        Notification notification = createProgressNotification(
                "repair:999",
                "Repair session 1 on range (1,100] for keyspace finished",
                RepairTask.ProgressEventType.PROGRESS.ordinal());

        handler.handleNotification(notification, null);

        assertThat(myRangeResults).isEmpty();
    }

    private Notification createProgressNotification(String source, String message, int type)
    {
        Notification notification = new Notification("progress", source, 1L, message);
        Map<String, Integer> userData = new HashMap<>();
        userData.put("type", type);
        notification.setUserData(userData);
        return notification;
    }

    private static class RangeResult
    {
        final LongTokenRange range;
        final RepairStatus status;

        RangeResult(LongTokenRange range, RepairStatus status)
        {
            this.range = range;
            this.status = status;
        }
    }
}
