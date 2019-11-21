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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeMetricHolder
{
    @Mock
    private TableStorageStates myTableStorageStates;

    private MetricRegistry myMetricRegistry = new MetricRegistry();

    private NodeMetricHolder myNodeMetricHolder;

    @Before
    public void init()
    {
        myNodeMetricHolder = new NodeMetricHolder(myMetricRegistry, myTableStorageStates);
    }

    @After
    public void cleanup()
    {
        myNodeMetricHolder.close();
    }

    @Test
    public void testDefaultMetrics()
    {
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(0);
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(0);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(1.0);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(1.0);
    }

    @Test
    public void testEmptyTableNotRepaired()
    {
        givenSingleRepairedTable(0, 0);

        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(0.0);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(1.0);
    }

    @Test
    public void testEmptyTablePartiallyRepaired()
    {
        givenSingleRepairedTable(0, 0.42);

        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(0.42);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(1.0);
    }

    @Test
    public void testNonEmptyTableNotRepaired()
    {
        givenSingleRepairedTable(3, 0);

        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(0.0);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(0.0);
    }

    @Test
    public void testUpdateRepairStateOneTable()
    {
        givenSingleRepairedTable(1234, 0.3);

        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(0.3);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(0.3);
    }

    @Test
    public void testUpdateRepairStateTwoTables()
    {
        long expectedDataSize = 1234;
        TableReference tableReference = new TableReference("keyspace", "table");
        double repairedRatio = 0.3;

        long expectedDataSizeTableTwo = 2345;
        TableReference tableReferenceTableTwo = new TableReference("keyspace", "table2");
        double repairedRatioTableTwo = 0.5;

        long expectedFullDataSize = expectedDataSize + expectedDataSizeTableTwo;
        double expectedFullRepairedDataRatio = (expectedDataSize * repairedRatio + expectedDataSizeTableTwo * repairedRatioTableTwo) / expectedFullDataSize;
        double expectedFullRepairedRatio = (repairedRatio + repairedRatioTableTwo) / 2;

        doReturn(expectedFullDataSize).when(myTableStorageStates).getDataSize();

        addRepairedTable(tableReference, repairedRatio, expectedDataSize);
        addRepairedTable(tableReferenceTableTwo, repairedRatioTableTwo, expectedDataSizeTableTwo);

        assertThat(getGauge(NodeMetricHolder.REPAIRED_TABLES).getValue()).isEqualTo(expectedFullRepairedRatio);
        assertThat(getGauge(NodeMetricHolder.REPAIRED_DATA).getValue()).isEqualTo(expectedFullRepairedDataRatio);
    }

    @Test
    public void testUpdateRepairTimingSuccessful()
    {
        long expectedTime = 1234;
        boolean successful = true;

        myNodeMetricHolder.repairTiming(expectedTime, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(1);
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(expectedTime);

        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(0);
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(0);
    }

    @Test
    public void testUpdateRepairTimingFailed()
    {
        long expectedTime = 1234;
        boolean successful = false;

        myNodeMetricHolder.repairTiming(expectedTime, TimeUnit.NANOSECONDS, successful);

        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_FAILED).getCount()).isEqualTo(1);
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_FAILED).getSnapshot().getMean()).isEqualTo(expectedTime);

        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_SUCCESS).getCount()).isEqualTo(0);
        assertThat(getTimer(NodeMetricHolder.REPAIR_TIMING_SUCCESS).getSnapshot().getMean()).isEqualTo(0);
    }

    @Test
    public void testGetRepairRatio()
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        addRepairedTable(tableReference, 0.3, 123);
        TableReference nonExistingRef = new TableReference("non", "existing");

        assertThat(myNodeMetricHolder.getRepairRatio(tableReference)).isEqualTo(0.3);
        assertThat(myNodeMetricHolder.getRepairRatio(nonExistingRef)).isNull();
    }

    private void addRepairedTable(TableReference tableReference, double repairedRatio, long dataSize)
    {
        doReturn(dataSize).when(myTableStorageStates).getDataSize(eq(tableReference));
        myNodeMetricHolder.repairState(tableReference, repairedRatio);
    }

    private Timer getTimer(String name)
    {
        return myMetricRegistry.getTimers().get(name);
    }

    private Gauge getGauge(String name)
    {
        return myMetricRegistry.getGauges().get(name);
    }

    private void givenSingleRepairedTable(long tableSize, double repairedRatio)
    {
        doReturn(tableSize).when(myTableStorageStates).getDataSize();

        TableReference tableReference = new TableReference("keyspace", "table");
        addRepairedTable(tableReference, repairedRatio, tableSize);
    }
}
