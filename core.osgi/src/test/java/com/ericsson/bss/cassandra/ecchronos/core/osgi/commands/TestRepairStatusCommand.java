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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.OutputData;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.SortBy;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.Status;
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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairStatusCommand
{
    private static final long REPAIR_INTERVAL = TimeUnit.DAYS.toMillis(7);
    private static final long REPAIR_WARNING_TIME = TimeUnit.DAYS.toMillis(8);
    private static final long REPAIR_ERROR_TIME = TimeUnit.DAYS.toMillis(10);

    private static final OutputData output1 = new OutputData(createTableRef("ks.tbl3"), Status.IN_QUEUE, 0.234, toMillis("1970-01-01T00:00Z"), toMillis("2019-12-31T23:59Z"));
    private static final OutputData output2 = new OutputData(createTableRef("ks.tbl2"), Status.COMPLETED, 0.0, toMillis("2019-12-24T12:34Z"), toMillis("2019-12-29T00:34Z"));
    private static final OutputData output3 = new OutputData(createTableRef("ks.tbl1"), Status.WARNING, 1.0, toMillis("2019-11-12T12:34Z"), toMillis("2019-12-29T12:34Z"));
    private static final OutputData output4 = new OutputData(createTableRef("ks.tbl4"), Status.ERROR, 0.456, toMillis("2029-11-12T23:59Z"), toMillis("2030-11-19T00:00Z"));
    private static final List<OutputData> outputData = asList(output1, output2, output3, output4);

    private static TableRepairMetricsProvider metricsMock = mock(TableRepairMetricsProvider.class);

    @BeforeClass
    public static void setup()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Test
    public void testRepairJobConversionToOutputData()
    {
        // Given
        Clock clockMock = mockClock("2019-12-12T12:34");
        long repairedAtForCompleted = toMillis("2019-12-12T00:00");
        long repairedAtForInQueue = repairedAtForCompleted - REPAIR_INTERVAL;
        long repairedAtForWarning = repairedAtForCompleted - REPAIR_WARNING_TIME;
        long repairedAtForError = repairedAtForCompleted - REPAIR_ERROR_TIME;

        RepairJobView job1 = mockRepairJob("ks.tbl1", 1.0, repairedAtForCompleted);
        RepairJobView job2 = mockRepairJob("ks.tbl2", null, repairedAtForInQueue);
        RepairJobView job3 = mockRepairJob("ks.tbl3", 0.339, repairedAtForWarning);
        RepairJobView job4 = mockRepairJob("ks.tbl4", 0.0, repairedAtForError);
        RepairScheduler schedulerMock = mockScheduler(asList(job1, job2, job3, job4));

        RepairStatusCommand command = new RepairStatusCommand(schedulerMock, metricsMock, clockMock);
        // When
        List<OutputData> outputData = command.getOutputData();
        // Then
        assertThat(outputData).extracting(OutputData::getTable, OutputData::getStatus, OutputData::getRatio, OutputData::getRepairedAt, OutputData::getNextRepair)
                .containsExactly(
                        tuple(createTableRef("ks.tbl1"), Status.COMPLETED, 1.0, repairedAtForCompleted, repairedAtForCompleted + REPAIR_INTERVAL),
                        tuple(createTableRef("ks.tbl2"), Status.IN_QUEUE, 0.0, repairedAtForInQueue, repairedAtForInQueue + REPAIR_INTERVAL),
                        tuple(createTableRef("ks.tbl3"), Status.WARNING, 0.339, repairedAtForWarning, repairedAtForWarning + REPAIR_INTERVAL),
                        tuple(createTableRef("ks.tbl4"), Status.ERROR, 0.0, repairedAtForError, repairedAtForError + REPAIR_INTERVAL)
                );
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(SortBy sortBy, boolean reverse, List<OutputData> expected)
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = sortBy;
        command.reverse = reverse;
        List<OutputData> outputs = asList(output1, output2, output3, output4);
        // When
        Comparator<OutputData> comparator = command.getOutputComparator();
        outputs.sort(comparator);
        // Then
        assertThat(outputs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {SortBy.TABLE_NAME, false, asList(output3, output2, output1, output4)},
                {SortBy.TABLE_NAME, true, asList(output4, output1, output2, output3)},
                {SortBy.STATUS, false, asList(output2, output1, output3, output4)},
                {SortBy.STATUS, true, asList(output4, output3, output1, output2)},
                {SortBy.REPAIRED_RATIO, false, asList(output2, output1, output4, output3)},
                {SortBy.REPAIRED_RATIO, true, asList(output3, output4, output1, output2)},
                {SortBy.REPAIRED_AT, false, asList(output1, output3, output2, output4)},
                {SortBy.REPAIRED_AT, true, asList(output4, output2, output3, output1)},
                {SortBy.NEXT_REPAIR, false, asList(output2, output3, output1, output4)},
                {SortBy.NEXT_REPAIR, true, asList(output4, output1, output3, output2)},
        };
    }

    @Test
    public void testPrintTableAllTablesSortedByNameWithFormattedOutput()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.TABLE_NAME;
        command.showAll = true;
        // When
        String output = executePrintTable(command, outputData);
        // Then
        String expected =
                "Table name │ Status    │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼───────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl1    │ WARNING   │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl2    │ COMPLETED │ 0%             │ 2019-12-24 12:34:00 │ 2019-12-29 00:34:00\n" +
                "ks.tbl3    │ IN_QUEUE  │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl4    │ ERROR     │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(output).isEqualTo(expected);
    }

    @Test
    public void testPrintTableSortedByStatusWithFormattedOutput()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.STATUS;
        // When
        String output = executePrintTable(command, outputData);
        // Then
        String expected =
                "Table name │ Status   │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼──────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl3    │ IN_QUEUE │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl1    │ WARNING  │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl4    │ ERROR    │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(output).isEqualTo(expected);
    }

    @Test
    public void testPrintTableAllTablesSortedByRepairedAtReversedLimitedAndNoFormat()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.REPAIRED_AT;
        command.reverse = true;
        command.noFormat = true;
        command.limit = 2;
        // When
        String output = executePrintTable(command, outputData);
        // Then
        String expected =
                "ks.tbl4   \tERROR  \t46%           \t2029-11-12 23:59:00\t2030-11-19 00:00:00\n" +
                "ks.tbl1   \tWARNING\t100%          \t2019-11-12 12:34:00\t2019-12-29 12:34:00\n";
        assertThat(output).isEqualTo(expected);
    }

    @Test
    public void testPrintTableWithSummaryOnlyFlag()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.summaryOnly = true;
        // When
        String output = executePrintTable(command, outputData);
        // Then
        assertThat(output).isEmpty();
    }

    @Test
    public void testPrintSummary()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<OutputData> summaryData = asList(output2, output2);
        // When
        String formattedOutput = executePrintSummary(command, summaryData);
        command.noFormat = true;
        String plainOutput = executePrintSummary(command, summaryData);
        // Then
        assertThat(formattedOutput).isEqualTo("Summary: 2 completed, 0 in queue\n");
        assertThat(plainOutput).isEqualTo("Summary: 2 completed, 0 in queue\n");
    }

    @Test
    public void testPrintSummaryWithWarnings()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<OutputData> summaryData = asList(output1, output3, output4, output3, output3, output4);
        // When
        String formattedOutput = executePrintSummary(command, summaryData);
        command.noFormat = true;
        String plainOutput = executePrintSummary(command, summaryData);
        // Then
        assertThat(formattedOutput).isEqualTo("Summary: 0 completed, 1 in queue, \u001B[33m3 warning\u001B[39m, \u001B[31m2 error\u001B[39m\n");
        assertThat(plainOutput).isEqualTo("Summary: 0 completed, 1 in queue, 3 warning, 2 error\n");
    }

    static TableReference createTableRef(String table)
    {
        String[] tableSplit = table.split("\\.");
        return new TableReference(tableSplit[0], tableSplit[1]);
    }

    private static long toMillis(String date)
    {
        return DateTime.parse(date).getMillis();
    }

    private Clock mockClock(String date)
    {
        long time = toMillis(date);
        Clock clockMock = mock(Clock.class);
        when(clockMock.millis()).thenReturn(time);
        return clockMock;
    }


    private static RepairJobView mockRepairJob(String table, Double repairRatio, long repairedAt)
    {
        TableReference tableReference = createTableRef(table);
        mockRepairRatio(repairRatio, tableReference);
        RepairStateSnapshot state = mockRepairedAt(repairedAt);
        RepairConfiguration repairConfiguration = createRepairConfiguration();
        return new RepairJobView(tableReference, repairConfiguration, state);
    }

    private RepairScheduler mockScheduler(List<RepairJobView> jobs)
    {
        RepairScheduler schedulerMock = mock(RepairScheduler.class);
        when(schedulerMock.getCurrentRepairJobs()).thenReturn(jobs);
        return schedulerMock;
    }

    private static void mockRepairRatio(Double repairRatio, TableReference tableReference)
    {
        when(metricsMock.getRepairRatio(eq(tableReference))).thenReturn(Optional.ofNullable(repairRatio));
    }

    private static RepairStateSnapshot mockRepairedAt(long repairedAt)
    {
        RepairStateSnapshot state = mock(RepairStateSnapshot.class);
        when(state.lastRepairedAt()).thenReturn(repairedAt);
        return state;
    }

    private static RepairConfiguration createRepairConfiguration()
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(REPAIR_INTERVAL, TimeUnit.MILLISECONDS)
                .withRepairWarningTime(REPAIR_WARNING_TIME, TimeUnit.MILLISECONDS)
                .withRepairErrorTime(REPAIR_ERROR_TIME, TimeUnit.MILLISECONDS)
                .build();
    }

    private String executePrintTable(RepairStatusCommand command, List<OutputData> data)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printTable(out, data);
        return os.toString();
    }

    private String executePrintSummary(RepairStatusCommand command, List<OutputData> data)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printSummary(out, data);
        return os.toString();
    }
}
