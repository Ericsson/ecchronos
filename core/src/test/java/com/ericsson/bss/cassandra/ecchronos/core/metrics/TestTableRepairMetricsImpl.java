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

import com.codahale.metrics.MetricRegistry;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestTableRepairMetricsImpl
{
    private static final String METRIC_TIMER_TYPE = "timers";
    private static final String METRIC_GAUGE_TYPE = "gauges";
    private static MBeanServer PLATFORM_MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    @Rule
    public TemporaryFolder metricsFolder = new TemporaryFolder();

    @Mock
    private TableStorageStates myTableStorageStates;

    private TableRepairMetricsImpl myTableRepairMetricsImpl;

    @Before
    public void init()
    {
        myTableRepairMetricsImpl = TableRepairMetricsImpl.builder()
                .withTableStorageStates(myTableStorageStates)
                .withStatisticsDirectory(metricsFolder.getRoot().getAbsolutePath())
                .withMetricRegistry(new MetricRegistry())
                .build();
    }

    @After
    public void cleanup()
    {
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
    public void testFullRepairedSingleTable() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");

        doReturn(1000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.report();

        double tablesRepaired = getMetricValue("TableRepairState", METRIC_GAUGE_TYPE);
        double dataRepaired = getMetricValue("DataRepairState", METRIC_GAUGE_TYPE);

        assertThat(tablesRepaired).isEqualTo(1.0);
        assertThat(dataRepaired).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedSingleTable() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");

        doReturn(1000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.report();

        double tablesRepaired = getMetricValue("TableRepairState", METRIC_GAUGE_TYPE);
        double dataRepaired = getMetricValue("DataRepairState", METRIC_GAUGE_TYPE);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.5);
    }

    @Test
    public void testFullRepairedTwoTables() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference tableReference2 = tableReference("keyspace", "table2");

        doReturn(2000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 0);
        myTableRepairMetricsImpl.report();

        double tablesRepaired = getMetricValue("TableRepairState", METRIC_GAUGE_TYPE);
        double dataRepaired = getMetricValue("DataRepairState", METRIC_GAUGE_TYPE);

        assertThat(tablesRepaired).isEqualTo(1.0);
        assertThat(dataRepaired).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedTwoTables() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference tableReference2 = tableReference("keyspace", "table2");

        doReturn(2000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 1);
        myTableRepairMetricsImpl.report();

        double tablesRepaired = getMetricValue("TableRepairState", METRIC_GAUGE_TYPE);
        double dataRepaired = getMetricValue("DataRepairState", METRIC_GAUGE_TYPE);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.5);
    }

    /**
     * A test case where there are two tables with different data sizes
     * and 75% of the data and 50% of the tables have been repaired.
     */
    @Test
    public void testOneRepairedOneNotRepairedTable() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference tableReference2 = tableReference("keyspace", "table2");

        doReturn(1500L).when(myTableStorageStates).getDataSize();
        doReturn(1125L).when(myTableStorageStates).getDataSize(eq(tableReference)); // 75%
        doReturn(375L).when(myTableStorageStates).getDataSize(eq(tableReference2)); // 25%

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 0, 1);
        myTableRepairMetricsImpl.report();

        double tablesRepaired = getMetricValue("TableRepairState", METRIC_GAUGE_TYPE);
        double dataRepaired = getMetricValue("DataRepairState", METRIC_GAUGE_TYPE);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.75);
    }

    @Test
    public void testLastRepairedAt() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference tableReference2 = tableReference("keyspace", "table2");
        long expectedLastRepaired = 1234567890L;
        long expectedLastRepaired2 = 9876543210L;

        myTableRepairMetricsImpl.lastRepairedAt(tableReference, expectedLastRepaired);
        myTableRepairMetricsImpl.lastRepairedAt(tableReference2, expectedLastRepaired2);
        myTableRepairMetricsImpl.report();

        double lastRepaired = getMetricValue(metricName(tableReference, "LastRepairedAt"), METRIC_GAUGE_TYPE);
        double lastRepaired2 = getMetricValue(metricName(tableReference2, "LastRepairedAt"), METRIC_GAUGE_TYPE);

        assertThat(lastRepaired).isEqualTo(expectedLastRepaired);
        assertThat(lastRepaired2).isEqualTo(expectedLastRepaired2);
    }

    @Test
    public void testRemainingRepairTime() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference tableReference2 = tableReference("keyspace", "table2");
        long expectedRemainingRepairTime = 10L;
        long expectedRemainingRepairTime2 = 20L;

        myTableRepairMetricsImpl.remainingRepairTime(tableReference, expectedRemainingRepairTime);
        myTableRepairMetricsImpl.remainingRepairTime(tableReference2, expectedRemainingRepairTime2);
        myTableRepairMetricsImpl.report();

        double remainingRepairTime = getMetricValue(metricName(tableReference, "RemainingRepairTime"), METRIC_GAUGE_TYPE);
        double remainingRepairTime2 = getMetricValue(metricName(tableReference2, "RemainingRepairTime"), METRIC_GAUGE_TYPE);

        assertThat(remainingRepairTime).isEqualTo(expectedRemainingRepairTime);
        assertThat(remainingRepairTime2).isEqualTo(expectedRemainingRepairTime2);
    }

    @Test
    public void testSuccessfulRepairTiming() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long expectedRepairTime = 1234L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.report();

        String metric = metricName(tableReference, "RepairSuccessTime");

        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 1, "Count")).isEqualTo(1);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 2, "Max")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 3, "Mean")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 4, "Min")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 5, "StdDev")).isEqualTo(0);

        assertPercentiles(metric, expectedRepairTime);
    }

    @Test
    public void testFailedRepairTiming() throws Exception
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long expectedRepairTime = 12345L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.report();

        String metric = metricName(tableReference, "RepairFailedTime");

        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 1, "Count")).isEqualTo(1);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 2, "Max")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 3, "Mean")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 4, "Min")).isEqualTo(expectedRepairTime);
        assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, 5, "StdDev")).isEqualTo(0);

        assertPercentiles(metric, expectedRepairTime);
    }

    @Test
    public void testGetRepairRatio()
    {
        TableReference tableReference = tableReference("keyspace", "table");
        TableReference nonExistingRef = tableReference("non", "existing");
        myTableRepairMetricsImpl.repairState(tableReference, 4, 1);

        assertThat(myTableRepairMetricsImpl.getRepairRatio(tableReference)).contains(0.8);
        assertThat(myTableRepairMetricsImpl.getRepairRatio(nonExistingRef)).isEmpty();
    }

    private void assertPercentiles(String metric, long expectedRepairTime) throws Exception
    {
        int csvPos = 6; // start csv position for percentiles

        List<String> percentiles = Arrays.asList("50", "75", "95", "98", "99", "999");

        for (String percentile : percentiles)
        {
            String percentileAttribute = percentile + "thPercentile";
            assertThat(getMetricValue(metric, METRIC_TIMER_TYPE, csvPos, percentileAttribute)).isEqualTo(expectedRepairTime);
            csvPos++;
        }
    }

    private double getMetricValue(String metric, String type) throws Exception
    {
        return getMetricValue(metric, type, 1, "Value");
    }

    private double getMetricValue(String metric, String type, int csvPos, String mBeanAttribute) throws Exception
    {
        double csvValue = getCsvMetricValue(metric, csvPos);
        Number mBeanValue = getMBeanValue(metric, type, mBeanAttribute);

        assertThat(csvValue).isEqualTo(mBeanValue.doubleValue());

        return csvValue;
    }

    private String metricName(TableReference tableReference, String metric)
    {
        return tableReference.getKeyspace() + "." + tableReference.getTable() + "-" + tableReference.getId() + "-" + metric;
    }

    private Number getMBeanValue(String metric, String type, String mBeanAttribute) throws Exception
    {
        ObjectName mBeanMetricName = new ObjectName("metrics:name=" + metric + ",type=" + type);

        return (Number) PLATFORM_MBEAN_SERVER.getAttribute(mBeanMetricName, mBeanAttribute);
    }

    private double getCsvMetricValue(String metric, int csvPos) throws IOException
    {
        String metricFile = metric + ".csv";
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(metricsFolder.getRoot(), metricFile))))
        {
            bufferedReader.readLine(); // CSV header
            String line = bufferedReader.readLine();

            String[] splits = line.split(",");

            return Double.parseDouble(splits[csvPos]);
        }
    }
}
