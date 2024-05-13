package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.StatusLogger;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestMetricInspector {

    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";
    @Mock
    private TableStorageStates myTableStorageStates;


    private MeterRegistry myMeterRegistry;
    private TableRepairMetricsImpl myTableRepairMetricsImpl;

    private MetricInspector myMetericInspector;

    private LoggerContext loggerContext;
    private ListAppender<ILoggingEvent> listAppender;




    @Before
    public void init() {
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
                .withTableStorageStates(myTableStorageStates)
                .withMeterRegistry(myMeterRegistry)
                .build();
        myMetericInspector = new MetricInspector(myMeterRegistry,
                1, 1);


    }

    @Test
    public void testInspectMeterRegistryForRepairFailuresWhenFailureThresholdIsBroken()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        TableReference tableReference1 = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        expectedRepairTime = 12345L;
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);

        myMetericInspector.inspectMeterRegistryForRepairFailures();


        List<ILoggingEvent> logsList = listAppender.list;

        long count = logsList.stream()
                .count();

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

        myMetericInspector.inspectMeterRegistryForRepairFailures();


        List<ILoggingEvent> logsList = listAppender.list;

        long count = logsList.stream()
                .count();

       assertEquals(0, count);
    }

}
