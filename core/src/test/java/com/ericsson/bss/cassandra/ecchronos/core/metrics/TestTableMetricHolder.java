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
    private final TableReference myTableReference = tableReference("keyspace", "table");

    private final MetricRegistry myMetricRegistry = new MetricRegistry();

    @Mock
    private NodeMetricHolder myNodeMetricHolder;

    private TableMetricHolder myTableMetricHolder;

    @Before
    public void init()
    {
        myTableMetricHolder = new TableMetricHolder(myTableReference, myMetricRegistry, myNodeMetricHolder);
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

        new TableMetricHolder(myTableReference, metricRegistry, myNodeMetricHolder);

        assertThat(metricRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void verifyMetricsAddedAfterInit()
    {
        MetricRegistry metricRegistry = new MetricRegistry();

        TableMetricHolder tableMetricHolder = new TableMetricHolder(myTableReference, metricRegistry, myNodeMetricHolder);
        tableMetricHolder.init();

        assertThat(metricRegistry.getMetrics().keySet()).containsExactlyInAnyOrder(
                metricName(TableMetricHolder.REMAINING_REPAIR_TIME),
                metricName(TableMetricHolder.LAST_REPAIRED_AT),
                metricName(TableMetricHolder.REPAIR_STATE),
                metricName(TableMetricHolder.REPAIR_TIMING_FAILED),
                metricName(TableMetricHolder.REPAIR_TIMING_SUCCESS));

        assertThat(getGague(TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(Double.NaN);
        assertThat(getGague(TableMetricHolder.LAST_REPAIRED_AT).getValue()).isEqualTo(0L);
        assertThat(getGague(TableMetricHolder.REMAINING_REPAIR_TIME).getValue()).isEqualTo(0L);
    }

    @Test
    public void testClose()
    {
        myTableMetricHolder.close();
        assertThat(myMetricRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void testUpdateLastRepairedAt()
    {
        long expectedLastRepaired = 1234;

        myTableMetricHolder.lastRepairedAt(expectedLastRepaired);

        assertThat(getGague(TableMetricHolder.LAST_REPAIRED_AT).getValue()).isEqualTo(expectedLastRepaired);
    }

    @Test
    public void testUpdateRemainingRepairTime()
    {
        long remainingRepairTime = 1234L;

        myTableMetricHolder.remainingRepairTime(remainingRepairTime);

        assertThat(getGague(TableMetricHolder.REMAINING_REPAIR_TIME).getValue()).isEqualTo(remainingRepairTime);
    }

    @Test
    public void testUpdateRepairStateIsPropagatedToNodeMetrics()
    {
        int expectedRepaired = 1;
        int expectedNotRepaired = 2;
        double expectedRatio = (double)expectedRepaired / (expectedNotRepaired + expectedRepaired);

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairStateAllRepaired()
    {
        int expectedRepaired = 1;
        int expectedNotRepaired = 0;
        double expectedRatio = 1;

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairStateNothingRepaired()
    {
        int expectedRepaired = 0;
        int expectedNotRepaired = 1;
        double expectedRatio = 0;

        myTableMetricHolder.repairState(expectedRepaired, expectedNotRepaired);

        assertThat(getGague(TableMetricHolder.REPAIR_STATE).getValue()).isEqualTo(expectedRatio);
        verify(myNodeMetricHolder).repairState(eq(myTableReference), eq(expectedRatio));
    }

    @Test
    public void testUpdateRepairTimingSuccessful()
    {
        boolean successful = true;
        long timeTaken = 1234;

        myTableMetricHolder.repairTiming(timeTaken, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(1);
        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(timeTaken);

        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(0);
        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(0);

        verify(myNodeMetricHolder).repairTiming(eq(timeTaken), eq(TimeUnit.NANOSECONDS), eq(successful));
    }

    @Test
    public void testUpdateRepairTimingFailed()
    {
        boolean successful = false;
        long timeTaken = 1234;

        myTableMetricHolder.repairTiming(timeTaken, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(1);
        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(timeTaken);

        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(0);
        assertThat(getTimer(TableMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(0);

        verify(myNodeMetricHolder).repairTiming(eq(timeTaken), eq(TimeUnit.NANOSECONDS), eq(successful));
    }

    private Timer getTimer(String name)
    {
        return myMetricRegistry.getTimers().get(metricName(name));
    }

    private Gauge getGague(String name)
    {
        return myMetricRegistry.getGauges().get(metricName(name));
    }

    private String metricName(String name)
    {
        return myTableReference.getKeyspace() + "." + myTableReference.getTable() + "-" + myTableReference.getId() + "-" + name;
    }
}
