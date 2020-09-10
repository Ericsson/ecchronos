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
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.SortBy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView.Status;

@RunWith(JUnitParamsRunner.class)
public class TestRepairStatusCommand
{
    private static final ScheduledRepairJob JOB1 = new ScheduledRepairJob(UUID.randomUUID(),"ks", "tbl3", Status.IN_QUEUE, 0.234, toMillis("1970-01-01T00:00:00Z"), toMillis("2019-12-31T23:59:00Z"), true);
    private static final ScheduledRepairJob JOB2 = new ScheduledRepairJob(UUID.randomUUID(),"ks", "tbl2", Status.COMPLETED, 0.0, toMillis("2019-12-24T12:34:00Z"), toMillis("2019-12-29T00:34:00Z"), true);
    private static final ScheduledRepairJob JOB3 = new ScheduledRepairJob(UUID.randomUUID(),"ks", "tbl1", Status.WARNING, 1.0, toMillis("2019-11-12T12:34:00Z"), toMillis("2019-12-29T12:34:00Z"), true);
    private static final ScheduledRepairJob JOB4 = new ScheduledRepairJob(UUID.randomUUID(),"ks", "tbl4", Status.ERROR, 0.456, toMillis("2029-11-12T23:59:00Z"), toMillis("2030-11-19T00:00:00Z"), true);
    private static final List<ScheduledRepairJob> JOBS = asList(JOB1, JOB2, JOB3, JOB4);

    @BeforeClass
    public static void setup()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(SortBy sortBy, boolean reverse, List<ScheduledRepairJob> expected)
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = sortBy;
        command.reverse = reverse;
        List<ScheduledRepairJob> jobs = asList(JOB1, JOB2, JOB3, JOB4);
        // When
        Comparator<ScheduledRepairJob> comparator = command.getJobComparator();
        jobs.sort(comparator);
        // Then
        assertThat(jobs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {SortBy.TABLE_NAME, false, asList(JOB3, JOB2, JOB1, JOB4)},
                {SortBy.TABLE_NAME, true, asList(JOB4, JOB1, JOB2, JOB3)},
                {SortBy.STATUS, false, asList(JOB2, JOB1, JOB3, JOB4)},
                {SortBy.STATUS, true, asList(JOB4, JOB3, JOB1, JOB2)},
                {SortBy.REPAIRED_RATIO, false, asList(JOB2, JOB1, JOB4, JOB3)},
                {SortBy.REPAIRED_RATIO, true, asList(JOB3, JOB4, JOB1, JOB2)},
                {SortBy.REPAIRED_AT, false, asList(JOB1, JOB3, JOB2, JOB4)},
                {SortBy.REPAIRED_AT, true, asList(JOB4, JOB2, JOB3, JOB1)},
                {SortBy.NEXT_REPAIR, false, asList(JOB2, JOB3, JOB1, JOB4)},
                {SortBy.NEXT_REPAIR, true, asList(JOB4, JOB1, JOB3, JOB2)},
        };
    }

    @Test
    public void testPrintTableAllTablesSortedByNameWithFormattedjob()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.TABLE_NAME;
        command.showAll = true;
        // When
        String job = executePrintTable(command);
        // Then
        String expected =
                "Table name │ Status    │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼───────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl1    │ WARNING   │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl2    │ COMPLETED │ 0%             │ 2019-12-24 12:34:00 │ 2019-12-29 00:34:00\n" +
                "ks.tbl3    │ IN_QUEUE  │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl4    │ ERROR     │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(job).isEqualTo(expected);
    }

    @Test
    public void testPrintTableSortedByStatusWithFormattedjob()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.STATUS;
        // When
        String job = executePrintTable(command);
        // Then
        String expected =
                "Table name │ Status   │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼──────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl3    │ IN_QUEUE │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl1    │ WARNING  │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl4    │ ERROR    │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(job).isEqualTo(expected);
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
        String job = executePrintTable(command);
        // Then
        String expected =
                "ks.tbl4   \tERROR  \t46%           \t2029-11-12 23:59:00\t2030-11-19 00:00:00\n" +
                "ks.tbl1   \tWARNING\t100%          \t2019-11-12 12:34:00\t2019-12-29 12:34:00\n";
        assertThat(job).isEqualTo(expected);
    }

    @Test
    public void testPrintTableWithSummaryOnlyFlag()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.summaryOnly = true;
        // When
        String job = executePrintTable(command);
        // Then
        assertThat(job).isEmpty();
    }

    @Test
    public void testPrintSummary()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<ScheduledRepairJob> jobs = asList(JOB2, JOB2);
        // When
        String formattedjob = executePrintSummary(command, jobs);
        command.noFormat = true;
        String plainjob = executePrintSummary(command, jobs);
        // Then
        assertThat(formattedjob).isEqualTo("Summary: 2 completed, 0 in queue\n");
        assertThat(plainjob).isEqualTo("Summary: 2 completed, 0 in queue\n");
    }

    @Test
    public void testPrintSummaryWithWarnings()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<ScheduledRepairJob> jobs = asList(JOB1, JOB3, JOB4, JOB3, JOB3, JOB4);
        // When
        String formattedjob = executePrintSummary(command, jobs);
        command.noFormat = true;
        String plainjob = executePrintSummary(command, jobs);
        // Then
        assertThat(formattedjob).isEqualTo("Summary: 0 completed, 1 in queue, \u001B[33m3 warning\u001B[39m, \u001B[31m2 error\u001B[39m\n");
        assertThat(plainjob).isEqualTo("Summary: 0 completed, 1 in queue, 3 warning, 2 error\n");
    }

    static TableReference createTableRef(String table)
    {
        String[] tableSplit = table.split("\\.");
        return new TableReference(tableSplit[0], tableSplit[1]);
    }

    private static long toMillis(String date)
    {
        return Instant.parse(date).toEpochMilli();
    }

    private String executePrintTable(RepairStatusCommand command)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printTable(out, JOBS);
        return os.toString();
    }

    private String executePrintSummary(RepairStatusCommand command, List<ScheduledRepairJob> jobs)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printSummary(out, jobs);
        return os.toString();
    }
}
