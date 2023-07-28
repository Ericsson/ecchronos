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

import com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.SortBy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduleConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairStatusCommand
{
    private static final Schedule SCHEDULE1 = new Schedule(UUID.randomUUID(), "ks", "tbl3",
            ScheduledRepairJobView.Status.ON_TIME, 0.234, toMillis("1970-01-01T00:00:00Z"),
            toMillis("2019-12-31T23:59:00Z"), new ScheduleConfig(), RepairOptions.RepairType.VNODE);
    private static final Schedule SCHEDULE2 = new Schedule(UUID.randomUUID(), "ks", "tbl2",
            ScheduledRepairJobView.Status.COMPLETED, 0.0, toMillis("2019-12-24T12:34:00Z"),
            toMillis("2019-12-29T00:34:00Z"), new ScheduleConfig(), RepairOptions.RepairType.VNODE);
    private static final Schedule SCHEDULE3 = new Schedule(UUID.randomUUID(), "ks", "tbl1",
            ScheduledRepairJobView.Status.LATE, 1.0, toMillis("2019-11-12T12:34:00Z"), toMillis("2019-12-29T12:34:00Z"),
            new ScheduleConfig(), RepairOptions.RepairType.VNODE);
    private static final Schedule SCHEDULE4 = new Schedule(UUID.randomUUID(), "ks", "tbl4",
            ScheduledRepairJobView.Status.OVERDUE, 0.456, toMillis("2029-11-12T23:59:00Z"),
            toMillis("2030-11-19T00:00:00Z"), new ScheduleConfig(), RepairOptions.RepairType.VNODE);
    private static final List<Schedule> SCHEDULES = asList(SCHEDULE1, SCHEDULE2, SCHEDULE3, SCHEDULE4);

    @BeforeClass
    public static void setup()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(SortBy sortBy, boolean reverse, List<Schedule> expected)
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = sortBy;
        command.reverse = reverse;
        List<Schedule> schedules = asList(SCHEDULE1, SCHEDULE2, SCHEDULE3, SCHEDULE4);
        // When
        Comparator<Schedule> comparator = command.getScheduleComparator();
        schedules.sort(comparator);
        // Then
        assertThat(schedules).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {SortBy.TABLE_NAME, false, asList(SCHEDULE3, SCHEDULE2, SCHEDULE1, SCHEDULE4)},
                {SortBy.TABLE_NAME, true, asList(SCHEDULE4, SCHEDULE1, SCHEDULE2, SCHEDULE3)},
                {SortBy.STATUS, false, asList(SCHEDULE2, SCHEDULE1, SCHEDULE3, SCHEDULE4)},
                {SortBy.STATUS, true, asList(SCHEDULE4, SCHEDULE3, SCHEDULE1, SCHEDULE2)},
                {SortBy.REPAIRED_RATIO, false, asList(SCHEDULE2, SCHEDULE1, SCHEDULE4, SCHEDULE3)},
                {SortBy.REPAIRED_RATIO, true, asList(SCHEDULE3, SCHEDULE4, SCHEDULE1, SCHEDULE2)},
                {SortBy.REPAIRED_AT, false, asList(SCHEDULE1, SCHEDULE3, SCHEDULE2, SCHEDULE4)},
                {SortBy.REPAIRED_AT, true, asList(SCHEDULE4, SCHEDULE2, SCHEDULE3, SCHEDULE1)},
                {SortBy.NEXT_REPAIR, false, asList(SCHEDULE2, SCHEDULE3, SCHEDULE1, SCHEDULE4)},
                {SortBy.NEXT_REPAIR, true, asList(SCHEDULE4, SCHEDULE1, SCHEDULE3, SCHEDULE2)},
        };
    }

    @Test
    public void testPrintTableAllTablesSortedByNameWithFormattedschedule()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.TABLE_NAME;
        command.showAll = true;
        // When
        String schedule = executePrintTable(command);
        // Then
        String expected =
                "Table name │ Status    │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼───────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl1    │ LATE      │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl2    │ COMPLETED │ 0%             │ 2019-12-24 12:34:00 │ 2019-12-29 00:34:00\n" +
                "ks.tbl3    │ ON_TIME   │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl4    │ OVERDUE   │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(schedule).isEqualTo(expected);
    }

    @Test
    public void testPrintTableSortedByStatusWithFormattedschedule()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.sortBy = SortBy.STATUS;
        // When
        String schedule = executePrintTable(command);
        // Then
        String expected =
                "Table name │ Status  │ Repaired ratio │ Repaired at         │ Next repair\n" +
                "───────────┼─────────┼────────────────┼─────────────────────┼────────────────────\n" +
                "ks.tbl3    │ ON_TIME │ 23%            │ 1970-01-01 00:00:00 │ 2019-12-31 23:59:00\n" +
                "ks.tbl1    │ LATE    │ 100%           │ 2019-11-12 12:34:00 │ 2019-12-29 12:34:00\n" +
                "ks.tbl4    │ OVERDUE │ 46%            │ 2029-11-12 23:59:00 │ 2030-11-19 00:00:00\n";
        assertThat(schedule).isEqualTo(expected);
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
        String schedule = executePrintTable(command);
        // Then
        String expected =
                "ks.tbl4   \tOVERDUE\t46%           \t2029-11-12 23:59:00\t2030-11-19 00:00:00\n" +
                "ks.tbl1   \tLATE   \t100%          \t2019-11-12 12:34:00\t2019-12-29 12:34:00\n";
        assertThat(schedule).isEqualTo(expected);
    }

    @Test
    public void testPrintTableWithSummaryOnlyFlag()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        command.summaryOnly = true;
        // When
        String schedule = executePrintTable(command);
        // Then
        assertThat(schedule).isEmpty();
    }

    @Test
    public void testPrintSummary()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<Schedule> schedules = asList(SCHEDULE2, SCHEDULE2);
        // When
        String formattedschedule = executePrintSummary(command, schedules);
        command.noFormat = true;
        String plainschedule = executePrintSummary(command, schedules);
        // Then
        assertThat(formattedschedule).isEqualTo("Summary: 2 completed, 0 on time\n");
        assertThat(plainschedule).isEqualTo("Summary: 2 completed, 0 on time\n");
    }

    @Test
    public void testPrintSummaryWithWarnings()
    {
        // Given
        RepairStatusCommand command = new RepairStatusCommand();
        List<Schedule> schedules = asList(SCHEDULE1, SCHEDULE3, SCHEDULE4, SCHEDULE3, SCHEDULE3, SCHEDULE4);
        // When
        String formattedschedule = executePrintSummary(command, schedules);
        command.noFormat = true;
        String plainschedule = executePrintSummary(command, schedules);
        // Then
        assertThat(formattedschedule).isEqualTo("Summary: 0 completed, 1 on time, \u001B[33m3 late\u001B[39m, \u001B[31m2 overdue\u001B[39m\n");
        assertThat(plainschedule).isEqualTo("Summary: 0 completed, 1 on time, 3 late, 2 overdue\n");
    }

    static TableReference createTableRef(String table)
    {
        String[] tableSplit = table.split("\\.");
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(tableSplit[0]);
        when(tableReference.getTable()).thenReturn(tableSplit[1]);
        return tableReference;
    }

    private static long toMillis(String date)
    {
        return Instant.parse(date).toEpochMilli();
    }

    private String executePrintTable(RepairStatusCommand command)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printTable(out, SCHEDULES);
        return os.toString();
    }

    private String executePrintSummary(RepairStatusCommand command, List<Schedule> schedules)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        command.printSummary(out, schedules);
        return os.toString();
    }
}
