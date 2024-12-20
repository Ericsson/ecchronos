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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestStatusLogger
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";

    private MeterRegistry myMeterRegistry;
    private TableRepairMetricsImpl myTableRepairMetricsImpl;

    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;

    @Before
    public void init()
    {
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        loggerContext.getLogger(StatusLogger.class).addAppender(listAppender);
        // Use composite registry here to simulate real world scenario where we have multiple registries
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        // Need at least one registry present in composite to record metrics
        compositeMeterRegistry.add(new SimpleMeterRegistry());
        myMeterRegistry = compositeMeterRegistry;
        myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                .withMeterRegistry(myMeterRegistry)
                .build();
    }

    @After
    public void cleanup()
    {
        myMeterRegistry.close();
        myTableRepairMetricsImpl.close();
    }

    @Test
    public void testLogForFailedRepairSessionCountLogs()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        StatusLogger.log(myMeterRegistry);
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Total repair failures in node till now is: 1", logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
    }

    @Test
    public void testLogForSuccessfulRepairSessionCountLogs()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, true);
        StatusLogger.log(myMeterRegistry);
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Total repair success in node till now is: 1", logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
    }

    @Test
    public void testLogForRepairedRatioStatusLogs()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        StatusLogger.log(myMeterRegistry);
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Node repair ratio is: 1.0", logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
    }

    @Test
    public void testLogForLastRepairedAtStatusLogs()
    {
        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long timeNow = System.currentTimeMillis();
        long timeDiff1 = 1000L;
        long expectedLastRepaired1 = timeNow - timeDiff1;
        long timeDiff2 = 5000L;
        long expectedLastRepaired2 = timeNow - timeDiff2;
        myTableRepairMetricsImpl.lastRepairedAt(tableReference1, expectedLastRepaired1);
        myTableRepairMetricsImpl.lastRepairedAt(tableReference2, expectedLastRepaired2);
        StatusLogger.log(myMeterRegistry);
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Node last repaired at", logMessage.substring(0, 21));
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
    }

    @Test
    public void testLogForNodeRemainingRepairTimeStatusLogs()
    {
        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long expectedRemainingRepairTime1 = 10L;
        long expectedRemainingRepairTime2 = 20L;
        myTableRepairMetricsImpl.remainingRepairTime(tableReference1, expectedRemainingRepairTime1);
        myTableRepairMetricsImpl.remainingRepairTime(tableReference2, expectedRemainingRepairTime2);
        StatusLogger.log(myMeterRegistry);
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        Double expectedRepairTime = (double) (expectedRemainingRepairTime1 + expectedRemainingRepairTime2) / 1000;
        assertEquals("Remaining time for node repair: ".concat(expectedRepairTime.toString()), logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
    }
}
