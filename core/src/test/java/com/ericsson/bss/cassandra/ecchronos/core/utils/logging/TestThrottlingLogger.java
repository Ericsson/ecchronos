/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.utils.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestThrottlingLogger
{
    private static final String TEST_LOG_MESSAGE = "TEST";
    private static final IllegalArgumentException TEST_EXCEPTION = new IllegalArgumentException("TEST");
    private static final long THROTTLE_INTERVAL = 1;
    private static final TimeUnit THROTTLE_INTERVAL_UNIT = TimeUnit.MINUTES;
    private static final int DEFAULT_LOG_COUNT = 100;

    @Test
    public void testInfo()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.info(TEST_LOG_MESSAGE);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.INFO);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testWarn()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.warn(TEST_LOG_MESSAGE);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.WARN);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testError()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.error(TEST_LOG_MESSAGE);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.ERROR);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testInfoWithException()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.info(TEST_LOG_MESSAGE, TEST_EXCEPTION);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.INFO);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testWarnWithException()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.warn(TEST_LOG_MESSAGE, TEST_EXCEPTION);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.WARN);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testErrorWithException()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, THROTTLE_INTERVAL_UNIT);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.error(TEST_LOG_MESSAGE, TEST_EXCEPTION);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.ERROR);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testInfoLowThrottle()
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, TimeUnit.NANOSECONDS);
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            throttlingLogger.info(TEST_LOG_MESSAGE);
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(DEFAULT_LOG_COUNT);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.INFO);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }

    @Test
    public void testThreadSafety() throws InterruptedException
    {
        Logger unittestLogger = (Logger) LoggerFactory.getLogger(TestThrottlingLogger.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        unittestLogger.addAppender(listAppender);

        ThrottlingLogger throttlingLogger = new ThrottlingLogger(unittestLogger, THROTTLE_INTERVAL, TimeUnit.MINUTES);
        CountDownLatch latch = new CountDownLatch(1);
        Set<Thread> threads = new HashSet<>();
        for (int i = 0; i < DEFAULT_LOG_COUNT; i++)
        {
            threads.add(new Thread(()-> {
                try
                {
                    latch.await();
                    throttlingLogger.info(TEST_LOG_MESSAGE);
                }
                catch (InterruptedException ie)
                {
                    //DO NOTHING
                }
            }, "TestThread" + i));
        }
        // Start all threads at once
        for (Thread thread : threads)
        {
            thread.start();
        }
        // Release the latch so all threads can start logging
        latch.countDown();
        // Wait for all threads to finish
        for (Thread thread : threads)
        {
            thread.join();
        }

        List<ILoggingEvent> logs = listAppender.list;
        assertThat(logs.size()).isEqualTo(1);
        assertThat(logs.get(0).getLevel()).isEqualTo(Level.INFO);
        assertThat(logs.get(0).getMessage()).isEqualTo(TEST_LOG_MESSAGE);
    }
}
