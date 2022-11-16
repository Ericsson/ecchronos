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

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestTableRepairMetricsImpl
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";

    @Mock
    private TableStorageStates myTableStorageStates;

    private TableRepairMetricsImpl myTableRepairMetricsImpl;
    private MeterRegistry myMeterRegistry;

    @Before
    public void init()
    {
        // Use composite registry here to simulate real world scenario where we have multiple registries
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        // Need at least one registry present in composite to record metrics
        compositeMeterRegistry.add(new SimpleMeterRegistry());
        myMeterRegistry = compositeMeterRegistry;
        myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                .withTableStorageStates(myTableStorageStates)
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
    public void testBuildWithNullTableStorageStates()
    {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> TableRepairMetricsImpl.builder()
                        .withTableStorageStates(null)
                        .build());
    }

    @Test
    public void testFullRepairedSingleTable()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        Gauge repairRatio = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairRatio).isNotNull();
        assertThat(repairRatio.value()).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedSingleTable()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);

        Gauge repairRatio = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairRatio).isNotNull();
        assertThat(repairRatio.value()).isEqualTo(0.5);
    }

    @Test
    public void testFullRepairedTwoTables()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 0);

        Gauge repairRatioTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairRatioTable1).isNotNull();
        assertThat(repairRatioTable1.value()).isEqualTo(1.0);

        Gauge repairRatioTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(repairRatioTable2).isNotNull();
        assertThat(repairRatioTable2.value()).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedTwoTables()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 1);

        Gauge repairRatioTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairRatioTable1).isNotNull();
        assertThat(repairRatioTable1.value()).isEqualTo(0.5);

        Gauge repairRatioTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(repairRatioTable2).isNotNull();
        assertThat(repairRatioTable2.value()).isEqualTo(0.5);
    }

    @Test
    public void testLastRepairedAt()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long expectedLastRepaired = 1234567890L;
        long expectedLastRepaired2 = 9876543210L;

        myTableRepairMetricsImpl.lastRepairedAt(tableReference, expectedLastRepaired);
        myTableRepairMetricsImpl.lastRepairedAt(tableReference2, expectedLastRepaired2);

        Gauge lastRepairedAtTable1 = myMeterRegistry.find(TableRepairMetricsImpl.LAST_REPAIRED_AT)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(lastRepairedAtTable1).isNotNull();
        assertThat(lastRepairedAtTable1.value()).isEqualTo((double) expectedLastRepaired/1000); // Based on metric registry this is converted to seconds/ms/etc.

        Gauge lastRepairedAtTable2 = myMeterRegistry.find(TableRepairMetricsImpl.LAST_REPAIRED_AT)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(lastRepairedAtTable2).isNotNull();
        assertThat(lastRepairedAtTable2.value()).isEqualTo((double) expectedLastRepaired2/1000); // Based on metric registry this is converted to seconds/ms/etc.
    }

    @Test
    public void testRemainingRepairTime()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long expectedRemainingRepairTime = 10L;
        long expectedRemainingRepairTime2 = 20L;

        myTableRepairMetricsImpl.remainingRepairTime(tableReference, expectedRemainingRepairTime);
        myTableRepairMetricsImpl.remainingRepairTime(tableReference2, expectedRemainingRepairTime2);

        Gauge remainingRepairTimeTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REMAINING_REPAIR_TIME)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(remainingRepairTimeTable1).isNotNull();
        assertThat(remainingRepairTimeTable1.value()).isEqualTo((double) expectedRemainingRepairTime/1000); // Based on metric registry this is converted to seconds/ms/etc.

        Gauge remainingRepairTimeTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REMAINING_REPAIR_TIME)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(remainingRepairTimeTable2).isNotNull();
        assertThat(remainingRepairTimeTable2.value()).isEqualTo((double) expectedRemainingRepairTime2/1000); // Based on metric registry this is converted to seconds/ms/etc.
    }

    @Test
    public void testSuccessfulRepairTiming()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 1234L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, true);

        Timer repairTime = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_TIME_TAKEN)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(repairTime).isNotNull();
        assertThat(repairTime.count()).isEqualTo(1);
        assertThat(repairTime.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(repairTime.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
    }

    @Test
    public void testFailedRepairTiming()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);

        Timer repairTime = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_TIME_TAKEN)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "false")
                .timer();
        assertThat(repairTime).isNotNull();
        assertThat(repairTime.count()).isEqualTo(1);
        assertThat(repairTime.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(repairTime.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
    }

    /**
     * Test that marks repair for one table as successful and failed for other table.
     */
    @Test
    public void testRepairTimingOneFailedAndOneSuccessful()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long successfulRepairTime = 12345L;
        long failedRepairTime = 123456L;

        myTableRepairMetricsImpl.repairTiming(tableReference, successfulRepairTime, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairTiming(tableReference2, failedRepairTime, TimeUnit.MILLISECONDS, false);

        Timer repairTime1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_TIME_TAKEN)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(repairTime1).isNotNull();
        assertThat(repairTime1.count()).isEqualTo(1);
        assertThat(repairTime1.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);
        assertThat(repairTime1.mean(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);

        Timer repairTime2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_TIME_TAKEN)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2, "successful", "false")
                .timer();
        assertThat(repairTime2).isNotNull();
        assertThat(repairTime2.count()).isEqualTo(1);
        assertThat(repairTime2.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);
        assertThat(repairTime2.mean(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);
    }
}
