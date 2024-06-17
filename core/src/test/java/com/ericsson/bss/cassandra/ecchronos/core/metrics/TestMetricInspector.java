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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.StatusLogger;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.junit.Assert.assertEquals;


@RunWith(MockitoJUnitRunner.class)
public class TestMetricInspector
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";

    @Mock
    private TableStorageStates myTableStorageStates;
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
                .withTableStorageStates(myTableStorageStates)
                .withMeterRegistry(myMeterRegistry)
                .build();
        myMetricInspector = new MetricInspector(myMeterRegistry, 1,  1, 5000);    }

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
        assertEquals(1, count);
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
    public void testStartInspectionMethod() throws InterruptedException
    {
        assertEquals(0, myMetricInspector.getTotalRecordFailures());
        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference1, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        List<ILoggingEvent> logsList = listAppender.list;
        myMetricInspector.startInspection();
        Thread.sleep(5000);
        long count = logsList.stream().count();
        String logMessage = logsList.get(0).getFormattedMessage();
        Level logLevel = logsList.get(0).getLevel();
        assertEquals("Total repair failures in node till now is: 2", logMessage);
        assertEquals(Level.DEBUG, logLevel);
        assertEquals(1, count);
        assertEquals(2, myMetricInspector.getTotalRecordFailures());
        myMetricInspector.stopInspection();
    }
}
