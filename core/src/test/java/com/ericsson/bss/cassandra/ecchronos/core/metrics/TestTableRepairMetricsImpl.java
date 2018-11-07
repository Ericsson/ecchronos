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

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class TestTableRepairMetricsImpl
{
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
                .build();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testBuildWithNullTableStorageStates()
    {
        TableRepairMetricsImpl.builder()
                .withTableStorageStates(null)
                .build();
    }

    @Test
    public void testFullRepairedSingleTable() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");

        doReturn(1000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.close();

        double tablesRepaired = getMetricValue("TableRepairState.csv", 1);
        double dataRepaired = getMetricValue("DataRepairState.csv", 1);

        assertThat(tablesRepaired).isEqualTo(1.0);
        assertThat(dataRepaired).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedSingleTable() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");

        doReturn(1000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.close();

        double tablesRepaired = getMetricValue("TableRepairState.csv", 1);
        double dataRepaired = getMetricValue("DataRepairState.csv", 1);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.5);
    }

    @Test
    public void testFullRepairedTwoTables() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");

        doReturn(2000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 0);
        myTableRepairMetricsImpl.close();

        double tablesRepaired = getMetricValue("TableRepairState.csv", 1);
        double dataRepaired = getMetricValue("DataRepairState.csv", 1);

        assertThat(tablesRepaired).isEqualTo(1.0);
        assertThat(dataRepaired).isEqualTo(1.0);
    }

    @Test
    public void testHalfRepairedTwoTables() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");

        doReturn(2000L).when(myTableStorageStates).getDataSize();
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference));
        doReturn(1000L).when(myTableStorageStates).getDataSize(eq(tableReference2));

        myTableRepairMetricsImpl.repairState(tableReference, 1, 1);
        myTableRepairMetricsImpl.repairState(tableReference2, 1, 1);
        myTableRepairMetricsImpl.close();

        double tablesRepaired = getMetricValue("TableRepairState.csv", 1);
        double dataRepaired = getMetricValue("DataRepairState.csv", 1);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.5);
    }

    /**
     * A test case where there are two tables with different data sizes
     * and 75% of the data and 50% of the tables have been repaired.
     */
    @Test
    public void testOneRepairedOneNotRepairedTable() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");

        doReturn(1500L).when(myTableStorageStates).getDataSize();
        doReturn(1125L).when(myTableStorageStates).getDataSize(eq(tableReference)); // 75%
        doReturn(375L).when(myTableStorageStates).getDataSize(eq(tableReference2)); // 25%

        myTableRepairMetricsImpl.repairState(tableReference, 1, 0);
        myTableRepairMetricsImpl.repairState(tableReference2, 0, 1);
        myTableRepairMetricsImpl.close();

        double tablesRepaired = getMetricValue("TableRepairState.csv", 1);
        double dataRepaired = getMetricValue("DataRepairState.csv", 1);

        assertThat(tablesRepaired).isEqualTo(0.5);
        assertThat(dataRepaired).isEqualTo(0.75);
    }

    @Test
    public void testLastRepairedAt() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");
        long expectedLastRepaired = 1234567890L;
        long expectedLastRepaired2 = 9876543210L;

        myTableRepairMetricsImpl.lastRepairedAt(tableReference, expectedLastRepaired);
        myTableRepairMetricsImpl.lastRepairedAt(tableReference2, expectedLastRepaired2);
        myTableRepairMetricsImpl.close();

        double lastRepaired = getMetricValue(tableReference + "-LastRepairedAt.csv", 1);
        double lastRepaired2 = getMetricValue(tableReference2 + "-LastRepairedAt.csv", 1);

        assertThat(lastRepaired).isEqualTo(expectedLastRepaired);
        assertThat(lastRepaired2).isEqualTo(expectedLastRepaired2);
    }

    @Test
    public void testSuccessfulRepairTiming() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long expectedRepairTime = 1234L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, true);
        myTableRepairMetricsImpl.close();

        String metricFile = tableReference + "-RepairSuccessTime.csv";

        assertThat(getMetricValue(metricFile, 1)).isEqualTo(1); // Count
        assertThat(getMetricValue(metricFile, 2)).isEqualTo(expectedRepairTime); // Max
        assertThat(getMetricValue(metricFile, 3)).isEqualTo(expectedRepairTime); // Mean
        assertThat(getMetricValue(metricFile, 4)).isEqualTo(expectedRepairTime); // Min
        assertThat(getMetricValue(metricFile, 5)).isEqualTo(0); // Stddev

        for (int i = 6; i <= 11; i++) // Percentiles
        {
            assertThat(getMetricValue(metricFile, i)).isEqualTo(expectedRepairTime);
        }
    }

    @Test
    public void testFailedRepairTiming() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long expectedRepairTime = 12345L;

        myTableRepairMetricsImpl.repairTiming(tableReference, expectedRepairTime, TimeUnit.MILLISECONDS, false);
        myTableRepairMetricsImpl.close();

        String metricFile = tableReference + "-RepairFailedTime.csv";

        assertThat(getMetricValue(metricFile, 1)).isEqualTo(1); // Count
        assertThat(getMetricValue(metricFile, 2)).isEqualTo(expectedRepairTime); // Max
        assertThat(getMetricValue(metricFile, 3)).isEqualTo(expectedRepairTime); // Mean
        assertThat(getMetricValue(metricFile, 4)).isEqualTo(expectedRepairTime); // Min
        assertThat(getMetricValue(metricFile, 5)).isEqualTo(0); // Stddev

        for (int i = 6; i <= 11; i++) // Percentiles
        {
            assertThat(getMetricValue(metricFile, i)).isEqualTo(expectedRepairTime);
        }
    }

    private double getMetricValue(String metricFile, int pos) throws IOException
    {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(metricsFolder.getRoot(), metricFile))))
        {
            bufferedReader.readLine(); // CSV header
            String line = bufferedReader.readLine();

            String[] splits = line.split(",");

            return Double.parseDouble(splits[pos]);
        }
    }
}
