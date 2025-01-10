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
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@RunWith(MockitoJUnitRunner.class)
public class TestTableRepairMetricsImpl
{
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_TABLE1 = "test_table1";
    private static final String TEST_TABLE2 = "test_table2";

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
                        .build());
    }

    @Test
    public void testFullRepairedSingleTable()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        Gauge repairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairedRatio).isNotNull();
        assertThat(repairedRatio.value()).isEqualTo(1.0);

        Gauge nodeRepairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIRED_RATIO)
                .gauge();
        assertThat(nodeRepairedRatio).isNotNull();
        assertThat(nodeRepairedRatio.value()).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedSingleTable()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);

        Gauge repairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairedRatio).isNotNull();
        assertThat(repairedRatio.value()).isEqualTo(0.5);

        Gauge nodeRepairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIRED_RATIO)
                .gauge();
        assertThat(nodeRepairedRatio).isNotNull();
        assertThat(nodeRepairedRatio.value()).isEqualTo(0.5);
    }

    @Test
    public void testFullRepairedTwoTables()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 0);

        Gauge repairedRatioTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairedRatioTable1).isNotNull();
        assertThat(repairedRatioTable1.value()).isEqualTo(1.0);

        Gauge repairedRatioTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(repairedRatioTable2).isNotNull();
        assertThat(repairedRatioTable2.value()).isEqualTo(1.0);

        Gauge nodeRepairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIRED_RATIO)
                .gauge();
        assertThat(nodeRepairedRatio).isNotNull();
        assertThat(nodeRepairedRatio.value()).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedTwoTables()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 1);

        Gauge repairedRatioTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairedRatioTable1).isNotNull();
        assertThat(repairedRatioTable1.value()).isEqualTo(0.5);

        Gauge repairedRatioTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(repairedRatioTable2).isNotNull();
        assertThat(repairedRatioTable2.value()).isEqualTo(0.5);

        Gauge nodeRepairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIRED_RATIO)
                .gauge();
        assertThat(nodeRepairedRatio).isNotNull();
        assertThat(nodeRepairedRatio.value()).isEqualTo(0.5);
    }

    @Test
    public void testOneRepairedAndOneHalfRepaired()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 1);

        Gauge repairedRatioTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(repairedRatioTable1).isNotNull();
        assertThat(repairedRatioTable1.value()).isEqualTo(1.0);

        Gauge repairedRatioTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIRED_RATIO)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(repairedRatioTable2).isNotNull();
        assertThat(repairedRatioTable2.value()).isEqualTo(0.5);

        Gauge nodeRepairedRatio = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIRED_RATIO)
                .gauge();
        assertThat(nodeRepairedRatio).isNotNull();
        assertThat(nodeRepairedRatio.value()).isEqualTo(0.75);
    }

    @Test
    public void testTimeSinceLastRepaired()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long timeNow = System.currentTimeMillis();
        TableRepairMetricsImpl.clock = () -> timeNow;

        long timeDiff = 1000L;
        long expectedLastRepaired = timeNow - timeDiff;
        long timeDiff2 = 5000L;
        long expectedLastRepaired2 = timeNow - timeDiff2;

        myTableRepairMetricsImpl.lastRepairedAt(tableReference, expectedLastRepaired);
        myTableRepairMetricsImpl.lastRepairedAt(tableReference2, expectedLastRepaired2);

        Gauge timeSinceLastRepairedTable1 = myMeterRegistry.find(TableRepairMetricsImpl.TIME_SINCE_LAST_REPAIRED)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1)
                .gauge();
        assertThat(timeSinceLastRepairedTable1).isNotNull();
        assertThat(timeSinceLastRepairedTable1.value()).isEqualTo((double)timeDiff/1000); // Based on metric registry this is converted to seconds/ms/etc.

        Gauge timeSinceLastRepairedTable2 = myMeterRegistry.find(TableRepairMetricsImpl.TIME_SINCE_LAST_REPAIRED)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2)
                .gauge();
        assertThat(timeSinceLastRepairedTable2).isNotNull();
        assertThat(timeSinceLastRepairedTable2.value()).isEqualTo((double)timeDiff2/1000); // Based on metric registry this is converted to seconds/ms/etc.

        Gauge nodeTimeSinceLastRepaired = myMeterRegistry.find(TableRepairMetricsImpl.NODE_TIME_SINCE_LAST_REPAIRED)
                .gauge();
        assertThat(nodeTimeSinceLastRepaired).isNotNull();
        assertThat(nodeTimeSinceLastRepaired.value()).isEqualTo((double)timeDiff2/1000); // Based on metric registry this is converted to seconds/ms/etc.
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

        Gauge nodeRemainingRepairTime = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REMAINING_REPAIR_TIME)
                .gauge();
        assertThat(nodeRemainingRepairTime).isNotNull();
        assertThat(nodeRemainingRepairTime.value()).isEqualTo((double) (expectedRemainingRepairTime+expectedRemainingRepairTime2)/1000); // Based on metric registry this is converted to seconds/ms/etc.
    }

    @Test
    public void testSuccessfulRepairSession()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 1234L;

        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, true);

        Timer repairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(repairSessions).isNotNull();
        assertThat(repairSessions.count()).isEqualTo(1);
        assertThat(repairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(repairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);

        Timer nodeRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        assertThat(nodeRepairSessions).isNotNull();
        assertThat(nodeRepairSessions.count()).isEqualTo(1);
        assertThat(nodeRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(nodeRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
    }

    @Test
    public void testMultipleSuccessfulRepairSessions()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime1 = 1000L;
        long expectedRepairTime2 = 2000L;
        long expectedMeanRepairTime = (expectedRepairTime1 + expectedRepairTime2) / 2;

        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime1, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime2, TimeUnit.MILLISECONDS, true);

        Timer repairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(repairSessions).isNotNull();
        assertThat(repairSessions.count()).isEqualTo(2);
        assertThat(repairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime2);
        assertThat(repairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedMeanRepairTime);

        Timer nodeRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        assertThat(nodeRepairSessions).isNotNull();
        assertThat(nodeRepairSessions.count()).isEqualTo(2);
        assertThat(nodeRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime2);
        assertThat(nodeRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedMeanRepairTime);
    }

    @Test
    public void testFailedRepairSession()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime = 12345L;

        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);

        Timer repairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "false")
                .timer();
        assertThat(repairSessions).isNotNull();
        assertThat(repairSessions.count()).isEqualTo(1);
        assertThat(repairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(repairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);

        Timer nodeRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        assertThat(nodeRepairSessions).isNotNull();
        assertThat(nodeRepairSessions.count()).isEqualTo(1);
        assertThat(nodeRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
        assertThat(nodeRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime);
    }

    @Test
    public void testMultipleFailedRepairSessions()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long expectedRepairTime1 = 1000L;
        long expectedRepairTime2 = 2000L;
        long expectedMeanRepairTime = (expectedRepairTime1 + expectedRepairTime2) / 2;

        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime1, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, expectedRepairTime2, TimeUnit.MILLISECONDS, false);

        Timer repairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "false")
                .timer();
        assertThat(repairSessions).isNotNull();
        assertThat(repairSessions.count()).isEqualTo(2);
        assertThat(repairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime2);
        assertThat(repairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedMeanRepairTime);

        Timer nodeRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        assertThat(nodeRepairSessions).isNotNull();
        assertThat(nodeRepairSessions.count()).isEqualTo(2);
        assertThat(nodeRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(expectedRepairTime2);
        assertThat(nodeRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedMeanRepairTime);
    }

    @Test
    public void testMultipleFailedAndSuccessfulRepairSessions()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        long successfulRepairTime1 = 1000L;
        long successfulRepairTime2 = 1000L;
        long meanSuccessfulRepairTime = (successfulRepairTime1 + successfulRepairTime2) / 2;
        long failedRepairTime1 = 500L;
        long failedRepairTime2 = 500L;
        long meanFailedRepairTime = (failedRepairTime1 + failedRepairTime2) / 2;

        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTime1, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTime2, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, failedRepairTime1, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, failedRepairTime2, TimeUnit.MILLISECONDS, false);

        Timer successfulRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(successfulRepairSessions).isNotNull();
        assertThat(successfulRepairSessions.count()).isEqualTo(2);
        assertThat(successfulRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime1);
        assertThat(successfulRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(meanSuccessfulRepairTime);

        Timer nodeSuccessfulRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        assertThat(nodeSuccessfulRepairSessions).isNotNull();
        assertThat(nodeSuccessfulRepairSessions.count()).isEqualTo(2);
        assertThat(nodeSuccessfulRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime1);
        assertThat(nodeSuccessfulRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(meanSuccessfulRepairTime);

        Timer failedRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "false")
                .timer();
        assertThat(failedRepairSessions).isNotNull();
        assertThat(failedRepairSessions.count()).isEqualTo(2);
        assertThat(failedRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime1);
        assertThat(failedRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(meanFailedRepairTime);

        Timer nodeFailedRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        assertThat(nodeFailedRepairSessions).isNotNull();
        assertThat(nodeFailedRepairSessions.count()).isEqualTo(2);
        assertThat(nodeFailedRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime1);
        assertThat(nodeFailedRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(meanFailedRepairTime);
    }

    /**
     * Test that marks repair for one table as successful and failed for other table.
     */
    @Test
    public void testRepairSessionOneFailedAndOneSuccessful()
    {
        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);
        long successfulRepairTime = 12345L;
        long failedRepairTime = 123456L;

        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTime, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference2, failedRepairTime, TimeUnit.MILLISECONDS, false);

        Timer successfulRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(successfulRepairSessions).isNotNull();
        assertThat(successfulRepairSessions.count()).isEqualTo(1);
        assertThat(successfulRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);
        assertThat(successfulRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);

        Timer failedRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2, "successful", "false")
                .timer();
        assertThat(failedRepairSessions).isNotNull();
        assertThat(failedRepairSessions.count()).isEqualTo(1);
        assertThat(failedRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);
        assertThat(failedRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);

        Timer nodeSuccessfulRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        assertThat(nodeSuccessfulRepairSessions).isNotNull();
        assertThat(nodeSuccessfulRepairSessions.count()).isEqualTo(1);
        assertThat(nodeSuccessfulRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);
        assertThat(nodeSuccessfulRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTime);

        Timer nodeFailedRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        assertThat(nodeFailedRepairSessions).isNotNull();
        assertThat(nodeFailedRepairSessions.count()).isEqualTo(1);
        assertThat(nodeFailedRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);
        assertThat(nodeFailedRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTime);
    }

    @Test
    public void testRepairSessionSomeFailedAndSomeSuccessfulForTwoTables()
    {
        long successfulRepairTimeTable1 = 295L;
        long successfulRepairTimeTable2 = 495L;

        long failedRepairTimeTable1 = 89L;
        long failedRepairTimeTable2 = 34L;

        TableReference tableReference = tableReference(TEST_KEYSPACE, TEST_TABLE1);
        TableReference tableReference2 = tableReference(TEST_KEYSPACE, TEST_TABLE2);

        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTimeTable1, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTimeTable1, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, successfulRepairTimeTable1, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference, failedRepairTimeTable1, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, failedRepairTimeTable1, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference, failedRepairTimeTable1, TimeUnit.MILLISECONDS, false);

        myTableRepairMetricsImpl.repairSession(tableReference2, successfulRepairTimeTable2, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference2, successfulRepairTimeTable2, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference2, successfulRepairTimeTable2, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.repairSession(tableReference2, failedRepairTimeTable2, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference2, failedRepairTimeTable2, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.repairSession(tableReference2, failedRepairTimeTable2, TimeUnit.MILLISECONDS, false);

        Timer successfulRepairSessionsTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "true")
                .timer();
        assertThat(successfulRepairSessionsTable1).isNotNull();
        assertThat(successfulRepairSessionsTable1.count()).isEqualTo(3);
        assertThat(successfulRepairSessionsTable1.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTimeTable1);
        assertThat(successfulRepairSessionsTable1.mean(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTimeTable1);

        Timer successfulRepairSessionsTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2, "successful", "true")
                .timer();
        assertThat(successfulRepairSessionsTable2).isNotNull();
        assertThat(successfulRepairSessionsTable2.count()).isEqualTo(3);
        assertThat(successfulRepairSessionsTable2.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTimeTable2);
        assertThat(successfulRepairSessionsTable2.mean(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTimeTable2);

        Timer failedRepairSessionsTable1 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE1, "successful", "false")
                .timer();
        assertThat(failedRepairSessionsTable1).isNotNull();
        assertThat(failedRepairSessionsTable1.count()).isEqualTo(3);
        assertThat(failedRepairSessionsTable1.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTimeTable1);
        assertThat(failedRepairSessionsTable1.mean(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTimeTable1);

        Timer failedRepairSessionsTable2 = myMeterRegistry.find(TableRepairMetricsImpl.REPAIR_SESSIONS)
                .tags("keyspace", TEST_KEYSPACE, "table", TEST_TABLE2, "successful", "false")
                .timer();
        assertThat(failedRepairSessionsTable2).isNotNull();
        assertThat(failedRepairSessionsTable2.count()).isEqualTo(3);
        assertThat(failedRepairSessionsTable2.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTimeTable2);
        assertThat(failedRepairSessionsTable2.mean(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTimeTable2);

        double expectedNodeMeanSuccessfulTime = (double) (successfulRepairTimeTable1 * 3 + successfulRepairTimeTable2 * 3) / 6;
        Timer nodeSuccessfulRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        assertThat(nodeSuccessfulRepairSessions).isNotNull();
        assertThat(nodeSuccessfulRepairSessions.count()).isEqualTo(6);
        assertThat(nodeSuccessfulRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(successfulRepairTimeTable2);
        assertThat(nodeSuccessfulRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedNodeMeanSuccessfulTime);

        double expectedNodeMeanFailedTime = (double) (failedRepairTimeTable1 * 3 + failedRepairTimeTable2 * 3) / 6;
        Timer nodeFailedRepairSessions = myMeterRegistry.find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        assertThat(nodeFailedRepairSessions).isNotNull();
        assertThat(nodeFailedRepairSessions.count()).isEqualTo(6);
        assertThat(nodeFailedRepairSessions.max(TimeUnit.MILLISECONDS)).isEqualTo(failedRepairTimeTable1);
        assertThat(nodeFailedRepairSessions.mean(TimeUnit.MILLISECONDS)).isEqualTo(expectedNodeMeanFailedTime);
    }
}
