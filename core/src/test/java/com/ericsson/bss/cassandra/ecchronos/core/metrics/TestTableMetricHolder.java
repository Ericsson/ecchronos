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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestTableMetricHolder
{
    private static final String NO_METRIC_PREFIX = "";
    private final TableReference myTableReference = tableReference("keyspace", "table");

    private final MetricRegistry myMetricRegistry = new MetricRegistry();

    @Mock
    private NodeMetricHolder myNodeMetricHolder;

    private TableMetricHolder myTableMetricHolder;

    @Before
    public void init()
    {
        myTableMetricHolder = new TableMetricHolder(myTableReference, myMetricRegistry, myNodeMetricHolder, NO_METRIC_PREFIX);
        myTableMetricHolder.init();
    }

    @After
    public void verifyNoOtherInteractions()
    {
        verifyNoMoreInteractions(ignoreStubs(myNodeMetricHolder));
    }

    /**
     * The reason this test case exists is that if we race during creation of a TableMetricHolder
     * the gauges might not get bound to the instance that is later used. {@link TableRepairMetricsImpl#tableMetricHolder(TableReference)}
     */
    @Test
    public void verifyNoMetricsAddedUntilInit()
    {
        MetricRegistry metricRegistry = new MetricRegistry();

        new TableMetricHolder(myTableReference, metricRegistry, myNodeMetricHolder, NO_METRIC_PREFIX);

        assertThat(metricRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void verifyMetricsAddedAfterInit()
    {
        MetricRegistry metricRegistry = new MetricRegistry();

        TableMetricHolder tableMetricHolder = new TableMetricHolder(myTableReference, metricRegistry,
                myNodeMetricHolder, NO_METRIC_PREFIX);
        tableMetricHolder.init();

        assertThat(metricRegistry.getMetrics().keySet()).containsExactlyInAnyOrder(
                metricName(TableMetricHolder.REMAINING_REPAIR_TIME),
                metricName(TableMetricHolder.LAST_REPAIRED_AT),
                metricName(TableMetricHolder.REPAIR_STATE),
                metricName(TableMetricHolder.REPAIR_TIMING_FAILED),
                metricName(TableMetricHolder.REPAIR_TIMING_SUCCESS),
                metricName(TableMetricHolder.FAILED_REPAIR_TASKS),
                metricName(TableMetricHolder.SUCCEEDED_REPAIR_TASKS));

        assertThat(getGague(metricRegistry, TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(Double.NaN);
        assertThat(getGague(metricRegistry, TableMetricHolder.LAST_REPAIRED_AT).getValue()).isEqualTo(0L);
        assertThat(getGague(metricRegistry, TableMetricHolder.REMAINING_REPAIR_TIME).getValue()).isEqualTo(0L);
        assertThat(getMeter(metricRegistry, TableMetricHolder.FAILED_REPAIR_TASKS).getCount()).isEqualTo(0L);
        assertThat(getMeter(metricRegistry, TableMetricHolder.SUCCEEDED_REPAIR_TASKS).getCount()).isEqualTo(0L);
    }

    @Test
    public void verifyMetricsAddedAfterInitWithPrefix()
    {
        MetricRegistry metricRegistry = new MetricRegistry();

        String prefix = "ecchronos";
        TableMetricHolder tableMetricHolder = new TableMetricHolder(myTableReference, metricRegistry,
                myNodeMetricHolder, prefix);
        tableMetricHolder.init();

        assertThat(metricRegistry.getMetrics().keySet()).containsExactlyInAnyOrder(
                metricName(prefix, TableMetricHolder.REMAINING_REPAIR_TIME),
                metricName(prefix, TableMetricHolder.LAST_REPAIRED_AT),
                metricName(prefix, TableMetricHolder.REPAIR_STATE),
                metricName(prefix, TableMetricHolder.REPAIR_TIMING_FAILED),
                metricName(prefix, TableMetricHolder.REPAIR_TIMING_SUCCESS),
                metricName(prefix, TableMetricHolder.FAILED_REPAIR_TASKS),
                metricName(prefix, TableMetricHolder.SUCCEEDED_REPAIR_TASKS));

        assertThat(getGague(metricRegistry, prefix, TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(Double.NaN);
        assertThat(getGague(metricRegistry, prefix, TableMetricHolder.LAST_REPAIRED_AT).getValue()).isEqualTo(0L);
        assertThat(getGague(metricRegistry, prefix, TableMetricHolder.REMAINING_REPAIR_TIME).getValue()).isEqualTo(0L);
        assertThat(getMeter(metricRegistry, prefix, TableMetricHolder.FAILED_REPAIR_TASKS).getCount()).isEqualTo(0L);
        assertThat(getMeter(metricRegistry, prefix, TableMetricHolder.SUCCEEDED_REPAIR_TASKS).getCount()).isEqualTo(0L);
    }

    @Test
    public void testClose()
    {
        myTableMetricHolder.close();
        assertThat(myMetricRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void testFailedRepairTask()
    {
        long failedRepairTasks = 5L;
        for (int i = 0; i < failedRepairTasks; i++)
        {
            myTableMetricHolder.failedRepairTask();
        }
        assertThat(getMeter(myMetricRegistry, TableMetricHolder.FAILED_REPAIR_TASKS).getCount()).isEqualTo(failedRepairTasks);
    }

    @Test
    public void testSucceededRepairTask()
    {
        long succeededRepairTasks = 5L;
        for (int i = 0; i < succeededRepairTasks; i++)
        {
            myTableMetricHolder.succeededRepairTask();
        }
        assertThat(getMeter(myMetricRegistry, TableMetricHolder.SUCCEEDED_REPAIR_TASKS).getCount()).isEqualTo(succeededRepairTasks);
    }

    @Test
    public void testUpdateLastRepairedAt()
    {
        long expectedLastRepaired = 1234;

        myTableMetricHolder.lastRepairedAt(expectedLastRepaired);

        assertThat(getGague(myMetricRegistry, TableMetricHolder.LAST_REPAIRED_AT).getValue()).isEqualTo(expectedLastRepaired);
    }

    @Test
    public void testUpdateRemainingRepairTime()
    {
        long remainingRepairTime = 1234L;

        myTableMetricHolder.remainingRepairTime(remainingRepairTime);

        assertThat(getGague(myMetricRegistry, TableMetricHolder.REMAINING_REPAIR_TIME).getValue()).isEqualTo(remainingRepairTime);
    }

    @Test
    public void testUpdateRepairStateIsPropagatedToNodeMetrics()
    {
        int expectedRepaired = 1;
        int expectedNotRepaired = 2;
        double expectedRatio = (double)expectedRepaired / (expectedNotRepaired + expectedRepaired);

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(myMetricRegistry, TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairStateAllRepaired()
    {
        int expectedRepaired = 1;
        int expectedNotRepaired = 0;
        double expectedRatio = 1;

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(myMetricRegistry, TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairStateNothingRepaired()
    {
        int expectedRepaired = 0;
        int expectedNotRepaired = 1;
        double expectedRatio = 0;

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(myMetricRegistry, TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairTimingSuccessful()
    {
        boolean successful = true;
        long timeTaken = 1234;

        myTableMetricHolder.repairTiming(timeTaken, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(1);
        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(timeTaken);

        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(0);
        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(0);

        verify(myNodeMetricHolder).repairTiming(eq(timeTaken), eq(TimeUnit.NANOSECONDS), eq(successful));
    }

    @Test
    public void testUpdateRepairTimingFailed()
    {
        boolean successful = false;
        long timeTaken = 1234;

        myTableMetricHolder.repairTiming(timeTaken, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(1);
        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(timeTaken);

        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(0);
        assertThat(getTimer(myMetricRegistry, TableMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(0);

        verify(myNodeMetricHolder).repairTiming(eq(timeTaken), eq(TimeUnit.NANOSECONDS), eq(successful));
    }

    private Timer getTimer(MetricRegistry metricRegistry, String prefix, String name)
    {
        return metricRegistry.getTimers().get(metricName(prefix, name));
    }

    private Meter getMeter(MetricRegistry metricRegistry, String prefix, String name)
    {
        return metricRegistry.getMeters().get(metricName(prefix, name));
    }

    private Gauge getGague(MetricRegistry metricRegistry, String prefix, String name)
    {
        return metricRegistry.getGauges().get(metricName(prefix, name));
    }

    private Timer getTimer(MetricRegistry metricRegistry, String name)
    {
        return metricRegistry.getTimers().get(metricName(name));
    }

    private Meter getMeter(MetricRegistry metricRegistry, String name)
    {
        return metricRegistry.getMeters().get(metricName(name));
    }

    private Gauge getGague(MetricRegistry metricRegistry, String name)
    {
        return metricRegistry.getGauges().get(metricName(name));
    }

    private String metricName(String name)
    {
        return myTableReference.getKeyspace() + "." + myTableReference.getTable() + "-" + myTableReference.getId()
                + "-" + name;
    }

    private String metricName(String prefix, String name)
    {
        return prefix + "." + myTableReference.getKeyspace() + "." + myTableReference.getTable() + "-"
                + myTableReference.getId() + "-" + name;
    }
}
