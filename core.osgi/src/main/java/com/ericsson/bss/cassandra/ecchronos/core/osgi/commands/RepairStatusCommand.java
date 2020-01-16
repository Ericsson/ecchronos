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
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob.Status;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.ansi.SimpleAnsi;
import org.apache.karaf.shell.support.table.ShellTable;

@Service
@Command(scope = "repair", name = "status", description = "Give the current repair status")
public class RepairStatusCommand implements Action
{
    @Option(name = "-l", aliases = "--limit", description = "Number of entries to display")
    int limit = Integer.MAX_VALUE;

    @Option(name = "-s", aliases = "--sort-by", description = "Sort output based on TABLE_NAME/STATUS/REPAIRED_RATIO/REPAIRED_AT/NEXT_REPAIR")
    SortBy sortBy = SortBy.TABLE_NAME;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order")
    boolean reverse = false;

    @Option(name = "-a", aliases = "--all", description = "Show status for all tables (including complete status)")
    boolean showAll = false;

    @Option(name = "--summary", description = "Show summary only")
    boolean summaryOnly = false;

    @Option(name = "--no-format", description = "Disable output formatting of table and colors")
    boolean noFormat = false;

    @Reference
    private RepairScheduler myRepairScheduler;

    @Override
    public Object execute() throws Exception
    {
        List<ScheduledRepairJob> jobs = getScheduledRepairJobs();
        printTable(System.out, jobs);
        printSummary(System.out, jobs);
        return null;
    }

    List<ScheduledRepairJob> getScheduledRepairJobs()
    {
        return myRepairScheduler.getCurrentRepairJobs()
                    .stream()
                    .map(ScheduledRepairJob::new)
                    .collect(Collectors.toList());
    }

    void printTable(PrintStream out, List<ScheduledRepairJob> jobs)
    {
        if (!summaryOnly)
        {
            ShellTable table = createShellTable();
            jobs.stream()
                    .filter(this::filterJob)
                    .sorted(getJobComparator())
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

    private boolean filterJob(ScheduledRepairJob job)
    {
        return showAll || job.status != Status.COMPLETED;
    }

    Comparator<ScheduledRepairJob> getJobComparator()
    {
        Comparator<ScheduledRepairJob> comparator;
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

    private static String getTableReference(String keyspace, String table)
    {
        return keyspace + "." + table;
    }

    List<Object> toRowContent(ScheduledRepairJob job)
        {
            return Arrays.asList(
                    getTableReference(job.keyspace, job.table),
                    job.status.name(),
                    PrintUtils.toPercentage(job.repairedRatio),
                    PrintUtils.epochToHumanReadable(job.lastRepairedAtInMs),
                    PrintUtils.epochToHumanReadable(job.nextRepairInMs));
        }

    void printSummary(PrintStream out, List<ScheduledRepairJob> jobs)
    {
        Map<Status, Long> stats = getStatusCount(jobs);

        StringBuilder sb = new StringBuilder("Summary: ");
        sb.append(stats.getOrDefault(Status.COMPLETED, 0L)).append(" completed, ");
        sb.append(stats.getOrDefault(Status.IN_QUEUE, 0L)).append(" in queue, ");
        sb.append(maybeCreateDescription(stats.get(Status.WARNING), SimpleAnsi.COLOR_YELLOW, " warning"));
        sb.append(maybeCreateDescription(stats.get(Status.ERROR), SimpleAnsi.COLOR_RED, " error"));
        sb.setLength(sb.length() - 2);

        out.println(sb.toString());
    }

    private Map<Status, Long> getStatusCount(List<ScheduledRepairJob> jobs)
    {
        return jobs.stream().collect(Collectors.groupingBy(job -> job.status, Collectors.counting()));
    }

    private String maybeCreateDescription(Long number, String color, String description)
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

    private String formatColor(String color)
    {
        return noFormat ? "" : color;
    }

    enum SortBy
    {
        TABLE_NAME, STATUS, REPAIRED_RATIO, REPAIRED_AT, NEXT_REPAIR;
    }
}
