/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.osgi.commands;

import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.OutputData;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairStatusCommand
{
    private static final OutputData output1 = new OutputData(createTableRef("ks1.tbl3"), Optional.of(0.2), 0, 3);
    private static final OutputData output2 = new OutputData(createTableRef("ks2.tbl2"), Optional.empty(), 2, 1);
    private static final OutputData output3 = new OutputData(createTableRef("ks1.tbl1"), Optional.of(0.3), 1, 2);

    private static TableRepairMetricsProvider metricsMock;
    private static RepairScheduler schedulerMock;

    @BeforeClass
    public static void setup()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        metricsMock = mock(TableRepairMetricsProvider.class);
        schedulerMock = mock(RepairScheduler.class);

        RepairJobView job1 = mockRepairJob("ks1.tbl3", 0.339, "2019-11-11T22:25Z", TimeUnit.SECONDS.toMillis(1));
        RepairJobView job2 = mockRepairJob("ks2.tbl2", null, "2019-12-24T14:57Z", TimeUnit.DAYS.toMillis(1));
        RepairJobView job3 = mockRepairJob("ks1.tbl1", 1.0, "2019-11-11T22:27Z", TimeUnit.MINUTES.toMillis(1));

        when(schedulerMock.getCurrentRepairJobs()).thenReturn(asList(job1, job2, job3));
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(String sortBy, boolean reverse, List<OutputData> expected)
    {
        // Given
        RepairStatusCommand command = createRepairStatusCommand(sortBy, reverse, 0);
        List<OutputData> outputs = asList(output1, output2, output3);
        // When
        Comparator<OutputData> comparator = command.getOutputComparator();
        outputs.sort(comparator);
        // Then
        assertThat(outputs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {"TABLE_NAME", false, asList(output3, output1, output2)},
                {"TABLE_NAME", true, asList(output2, output1, output3)},
                {"REPAIRED_RATIO", false, asList(output2, output1, output3)},
                {"REPAIRED_RATIO", true, asList(output3, output1, output2)},
                {"REPAIRED_AT", false, asList(output1, output3, output2)},
                {"REPAIRED_AT", true, asList(output2, output3, output1)},
                {"NEXT_REPAIR", false, asList(output2, output3, output1)},
                {"NEXT_REPAIR", true, asList(output1, output3, output2)},
                {"DEFAULT", false, asList(output3, output1, output2)},
        };
    }

    @Test
    public void testRepairStatusSortedByTableName()
    {
        // Given
        RepairStatusCommand command = createRepairStatusCommand("TABLE_NAME", false, 4);
        Comparator<OutputData> comparator = command.getOutputComparator();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printTables(out, comparator);
        // Then
        String expected =
                "Table name │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks1.tbl1   │ 100%           │ 2019-11-11 22:27:00 │ 2019-11-11 22:28:00\n" +
                "ks1.tbl3   │ 34%            │ 2019-11-11 22:25:00 │ 2019-11-11 22:25:01\n" +
                "ks2.tbl2   │ -              │ 2019-12-24 14:57:00 │ 2019-12-25 14:57:00\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    @Test
    public void testRepairStatusSortedByRepairedAtReversedAndLimited()
    {
        // Given
        RepairStatusCommand command = createRepairStatusCommand("REPAIRED_AT", true, 2);
        Comparator<OutputData> comparator = command.getOutputComparator();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printTables(out, comparator);
        // Then
        String expected =
                "Table name │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks2.tbl2   │ -              │ 2019-12-24 14:57:00 │ 2019-12-25 14:57:00\n" +
                "ks1.tbl1   │ 100%           │ 2019-11-11 22:27:00 │ 2019-11-11 22:28:00\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    private static RepairJobView mockRepairJob(String table, Double repairRatio, String repairedAt, long repairInterval)
    {
        TableReference tableReference = createTableRef(table);
        mockRepairRatio(repairRatio, tableReference);
        RepairStateSnapshot state = mockRepairedAt(repairedAt);
        RepairConfiguration repairConfiguration = mockRepairConfiguration(repairInterval);
        return new RepairJobView(tableReference, repairConfiguration, state);
    }

    static TableReference createTableRef(String table)
    {
        String[] tableSplit = table.split("\\.");
        return new TableReference(tableSplit[0], tableSplit[1]);
    }

    private static void mockRepairRatio(Double repairRatio, TableReference tableReference)
    {
        when(metricsMock.getRepairRatio(eq(tableReference))).thenReturn(Optional.ofNullable(repairRatio));
    }

    private static RepairStateSnapshot mockRepairedAt(String repairedAt)
    {
        RepairStateSnapshot state = mock(RepairStateSnapshot.class);
        long repairedAtMillis = DateTime.parse(repairedAt).getMillis();
        when(state.lastRepairedAt()).thenReturn(repairedAtMillis);
        return state;
    }

    private static RepairConfiguration mockRepairConfiguration(long repairInterval)
    {
        RepairConfiguration repairConfiguration = mock(RepairConfiguration.class);
        when(repairConfiguration.getRepairIntervalInMs()).thenReturn(repairInterval);
        return repairConfiguration;
    }

    private RepairStatusCommand createRepairStatusCommand(String sortBy, boolean reverse, int limit)
    {
        return new RepairStatusCommand(schedulerMock, metricsMock, sortBy, reverse, limit);
    }
}
