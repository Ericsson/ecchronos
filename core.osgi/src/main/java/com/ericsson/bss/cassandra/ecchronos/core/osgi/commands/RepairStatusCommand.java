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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.ansi.SimpleAnsi;
import org.apache.karaf.shell.support.table.ShellTable;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView.ScheduleStatus;

@Service
@Command(scope = "repair", name = "status", description = "Give the current repair status")
@SuppressWarnings("visibilitymodifier")
public class RepairStatusCommand implements Action
{
    @Option(name = "-l", aliases = "--limit",
            description = "Number of entries to display")
    int limit = Integer.MAX_VALUE;

    @Option(name = "-s", aliases = "--sort-by",
            description = "Sort output based on TABLE_NAME/STATUS/REPAIRED_RATIO/REPAIRED_AT/NEXT_REPAIR")
    SortBy sortBy = SortBy.TABLE_NAME;

    @Option(name = "-r", aliases = "--reverse",
            description = "Reverse the sort order")
    boolean reverse = false;

    @Option(name = "-a", aliases = "--all",
            description = "Show status for all tables (including complete status)")
    boolean showAll = false;

    @Option(name = "--summary",
            description = "Show summary only")
    boolean summaryOnly = false;

    @Option(name = "--no-format",
            description = "Disable output formatting of table and colors")
    boolean noFormat = false;

    @Reference
    private RepairScheduler myRepairScheduler;

    @Override
    public final Object execute() throws Exception
    {
        List<Schedule> schedules = getScheduledRepairJobs();
        printTable(System.out, schedules);
        printSummary(System.out, schedules);
        return null;
    }

    public final List<Schedule> getScheduledRepairJobs()
    {
        return myRepairScheduler.getCurrentRepairJobs()
                    .stream()
                    .map(Schedule::new)
                    .collect(Collectors.toList());
    }

    public final void printTable(final PrintStream out, final List<Schedule> schedules)
    {
        if (!summaryOnly)
        {
            ShellTable table = createShellTable();
            schedules.stream()
                    .filter(this::filterSchedule)
                    .sorted(getScheduleComparator())
                    .limit(limit)
                    .forEach(job -> table.addRow().addContent(toRowContent(job)));
            table.print(out, !noFormat);
        }
    }

    private ShellTable createShellTable()
    {
        ShellTable table = new ShellTable();
        table.column("Table name");
        table.column("Status");
        table.column("Repaired ratio");
        table.column("Repaired at");
        table.column("Next repair");
        return table;
    }

    private boolean filterSchedule(final Schedule schedule)
    {
        return showAll || schedule.status != ScheduleStatus.COMPLETED;
    }

    public final Comparator<Schedule> getScheduleComparator()
    {
        Comparator<Schedule> comparator;
        switch (sortBy)
        {
            case STATUS:
                comparator = Comparator.comparing(job -> job.status);
                break;
            case REPAIRED_RATIO:
                comparator = Comparator.comparing(job -> job.repairedRatio);
                break;
            case REPAIRED_AT:
                comparator = Comparator.comparing(job -> job.lastRepairedAtInMs);
                break;
            case NEXT_REPAIR:
                comparator = Comparator.comparing(job -> job.nextRepairInMs);
                break;
            case TABLE_NAME:
            default:
                comparator = Comparator.comparing(job -> getTableReference(job.keyspace, job.table));
                break;
        }

        return reverse
                ? comparator.reversed()
                : comparator;
    }

    private static String getTableReference(final String keyspace, final String table)
    {
        return keyspace + "." + table;
    }

    public final List<Object> toRowContent(final Schedule schedule)
        {
            return Arrays.asList(
                    getTableReference(schedule.keyspace, schedule.table),
                    schedule.status.name(),
                    PrintUtils.toPercentage(schedule.repairedRatio),
                    PrintUtils.epochToHumanReadable(schedule.lastRepairedAtInMs),
                    PrintUtils.epochToHumanReadable(schedule.nextRepairInMs));
        }

    public final void printSummary(final PrintStream out, final List<Schedule> schedules)
    {
        Map<ScheduleStatus, Long> stats = getStatusCount(schedules);

        StringBuilder sb = new StringBuilder("Summary: ");
        sb.append(stats.getOrDefault(ScheduleStatus.COMPLETED, 0L)).append(" completed, ");
        sb.append(stats.getOrDefault(ScheduleStatus.ON_TIME, 0L)).append(" on time, ");
        sb.append(maybeCreateDescription(stats.get(ScheduleStatus.LATE), SimpleAnsi.COLOR_YELLOW, " late"));
        sb.append(maybeCreateDescription(stats.get(ScheduleStatus.OVERDUE), SimpleAnsi.COLOR_RED, " overdue"));
        sb.setLength(sb.length() - 2);

        out.println(sb.toString());
    }

    private Map<ScheduleStatus, Long> getStatusCount(final List<Schedule> schedules)
    {
        return schedules.stream().collect(Collectors.groupingBy(schedule -> schedule.status, Collectors.counting()));
    }

    private String maybeCreateDescription(final Long number, final String color, final String description)
    {
        StringBuilder sb = new StringBuilder();
        if (number != null)
        {
            sb.append(formatColor(color))
                    .append(number)
                    .append(description)
                    .append(formatColor(SimpleAnsi.COLOR_DEFAULT))
                    .append(", ");
        }
        return sb.toString();
    }

    private String formatColor(final String color)
    {
        return noFormat ? "" : color;
    }

    enum SortBy
    {
        TABLE_NAME, STATUS, REPAIRED_RATIO, REPAIRED_AT, NEXT_REPAIR;
    }
}
