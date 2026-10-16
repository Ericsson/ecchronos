/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import static com.ericsson.bss.cassandra.ecchronos.core.metrics.MockTableReferenceFactory.tableReference;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.table.StatusLogger;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;


@RunWith(MockitoJUnitRunner.class)
public class TestMetricInspector
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final long SHORT_TRIGGER_INTERVAL_MS = 50L;

    private MeterRegistry myMeterRegistry;
    private TableRepairMetricsImpl myTableRepairMetricsImpl;
    private MetricInspector myMetricInspector;
    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;

    @Before
    public void init()
    {
        loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        loggerContext.getLogger(StatusLogger.class).addAppender(listAppender);
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        compositeMeterRegistry.add(new SimpleMeterRegistry());
        myMeterRegistry = compositeMeterRegistry;
        myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                .withMeterRegistry(myMeterRegistry)
                .build();
        myMetricInspector = new MetricInspector(myMeterRegistry, 1, 1, 5000);
    }

    @Test
    public void testInspectMeterRegistryForRepairFailuresWhenFailureThresholdIsBroken()
    {
        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myMetricInspector.inspectMeterRegistryForRepairFailures();
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Total repair failures in node till now is: 2", logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(4, count);
    }

    @Test
    public void testInspectMeterRegistryForRepairFailuresWhenFailureThresholdIsIntact()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myMetricInspector.inspectMeterRegistryForRepairFailures();
        List<ILoggingEvent> logsList = listAppender.list;
        long count = logsList.stream().count();
        assertEquals(0, count);
    }

    @Test
    public void testStartInspectionMethod()
    {
        // Use a short inspection interval so the background task fires promptly, and poll for the
        // resulting state instead of sleeping for a fixed duration. Sleeping a fixed amount and then
        // asserting an exact log/fire count is timing-dependent: on a loaded machine the scheduled
        // task may fire a different number of times before the sleep elapses, making the test flaky.
        MetricInspector inspector = new MetricInspector(myMeterRegistry, 1, 1, SHORT_TRIGGER_INTERVAL_MS);
        assertEquals(0, inspector.getTotalRecordFailures());

        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        List<ILoggingEvent> logsList = listAppender.list;

        inspector.startInspection();
        try
        {
            // Wait until the background inspection has observed both failures and emitted the status log.
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() ->
                    {
                        assertEquals(2, inspector.getTotalRecordFailures());
                        assertEquals(false, logsList.isEmpty());
                    });
        }
        finally
        {
            inspector.stopInspection();
        }

        // The first emitted status log reflects the two recorded failures.
        assertEquals("Total repair failures in node till now is: 2", logsList.get(0).getFormattedMessage());
        assertEquals(Level.DEBUG, logsList.get(0).getLevel());
        assertEquals(2, inspector.getTotalRecordFailures());
    }
}
