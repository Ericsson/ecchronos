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
import java.util.Optional;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
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
    @Option(name = "-l", aliases = "--limit", description = "Number of entries to display", required = false, multiValued = false)
    int limit = Integer.MAX_VALUE;

    @Option(name = "-s", aliases = "--sort_by", description = "Sort output based on TABLE_NAME/STATUS/RAPAIRED_RATIO/RAPAIRED_AT/NEXT_REPAIR", required = false, multiValued = false)
    SortBy sortBy = SortBy.TABLE_NAME;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order", required = false, multiValued = false)
    boolean reverse = false;

    @Option(name = "-a", aliases = "--all", description = "Show status for all tables (including complete status)", required = false, multiValued = false)
    boolean showAll = false;

    @Option(name = "--summary", description = "Show summary only", required = false, multiValued = false)
    boolean summaryOnly = false;

    @Option(name = "--no-format", description = "Disable output formatting of table and colors", required = false, multiValued = false)
    boolean noFormat = false;

    @Reference
    private RepairScheduler myRepairScheduler;

    @Reference
    private TableRepairMetricsProvider myTableRepairMetrics;

    private Clock myClock = Clock.DEFAULT;

    public RepairStatusCommand()
    {
    }

    @VisibleForTesting
    RepairStatusCommand(RepairScheduler repairScheduler, TableRepairMetricsProvider tableRepairMetrics, Clock clock)
    {
        myRepairScheduler = repairScheduler;
        myTableRepairMetrics = tableRepairMetrics;
        myClock = clock;
    }

    @Override
    public Object execute() throws Exception
    {
        List<OutputData> data = getOutputData();
        printTable(System.out, data, getOutputComparator());
        printSummary(System.out, data);
        return null;
    }

    List<OutputData> getOutputData()
    {
        return myRepairScheduler.getCurrentRepairJobs()
                    .stream()
                    .map(this::toOutputData)
                    .collect(Collectors.toList());
    }

    private OutputData toOutputData(RepairJobView job)
    {
        TableReference table = job.getTableReference();
        Status status = getStatus(job);
        Optional<Double> repairRatio = myTableRepairMetrics.getRepairRatio(table);
        long repairedAt = job.getRepairStateSnapshot().lastRepairedAt();
        long nextRepair = repairedAt + job.getRepairConfiguration().getRepairIntervalInMs();
        return new OutputData(table, status, repairRatio.orElse(0.0), repairedAt, nextRepair);
    }

    private Status getStatus(RepairJobView job)
    {
        long repairedAt = job.getRepairStateSnapshot().lastRepairedAt();
        long msSinceLastRepair = myClock.getTime() - repairedAt;
        RepairConfiguration config = job.getRepairConfiguration();

        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return Status.ERROR;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return Status.WARNING;
        }
        if (msSinceLastRepair >= config.getRepairIntervalInMs())
        {
            return Status.IN_QUEUE;
        }
        return Status.COMPLETED;
    }

    Comparator<OutputData> getOutputComparator()
    {
        Comparator<OutputData> comparator;
        switch (sortBy)
        {
            case STATUS:
                comparator = Comparator.comparing(OutputData::getStatus);
                break;
            case RAPAIRED_RATIO:
                comparator = Comparator.comparing(OutputData::getRatio);
                break;
            case RAPAIRED_AT:
                comparator = Comparator.comparing(OutputData::getRepairedAt);
                break;
            case NEXT_REPAIR:
                comparator = Comparator.comparing(OutputData::getNextRepair);
                break;
            case TABLE_NAME:
            default:
                comparator = Comparator.comparing(outputData -> outputData.getTable().toString());
                break;
        }

        return reverse
                ? comparator.reversed()
                : comparator;
    }

    void printTable(PrintStream out, List<OutputData> data, Comparator<OutputData> comparator)
    {
        if (!summaryOnly && !data.isEmpty())
        {
            ShellTable table = createShellTable();
            data.stream()
                    .filter(this::filterOutput)
                    .sorted(comparator)
                    .limit(limit)
                    .forEach(outputData -> table.addRow().addContent(outputData.toRowContent()));
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

    private boolean filterOutput(OutputData data)
    {
        return showAll || data.status != Status.COMPLETED;
    }

    void printSummary(PrintStream out, List<OutputData> data)
    {
        Map<Status, Long> stats = getStatusCount(data);

        StringBuilder sb = new StringBuilder("Summary: ");
        sb.append(stats.getOrDefault(Status.COMPLETED, 0L)).append(" completed, ");
        sb.append(stats.getOrDefault(Status.IN_QUEUE, 0L)).append(" in queue, ");
        sb.append(maybeCreateDescription(stats.get(Status.WARNING), SimpleAnsi.COLOR_YELLOW, " warning"));
        sb.append(maybeCreateDescription(stats.get(Status.ERROR), SimpleAnsi.COLOR_RED, " error"));
        sb.setLength(sb.length() - 2);

        out.println(sb.toString());
    }

    private Map<Status, Long> getStatusCount(List<OutputData> data)
    {
        return data.stream().collect(Collectors.groupingBy(outputData -> outputData.status, Collectors.counting()));
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
        TABLE_NAME, STATUS, RAPAIRED_RATIO, RAPAIRED_AT, NEXT_REPAIR;

    }

    enum Status
    {
        COMPLETED, IN_QUEUE, WARNING, ERROR;

    }

    static class OutputData
    {
        private TableReference table;
        private Status status;
        private Double ratio;
        private long repairedAt;
        private long nextRepair;

        OutputData(TableReference table, Status status, Double ratio, long repairedAt, long nextRepair)
        {
            this.table = table;
            this.status = status;
            this.ratio = ratio;
            this.repairedAt = repairedAt;
            this.nextRepair = nextRepair;
        }

        TableReference getTable()
        {
            return table;
        }

        Status getStatus()
        {
            return status;
        }

        Double getRatio()
        {
            return ratio;
        }

        long getRepairedAt()
        {
            return repairedAt;
        }

        long getNextRepair()
        {
            return nextRepair;
        }

        List<Object> toRowContent()
        {
            return Arrays.asList(
                    table,
                    status.name(),
                    PrintUtils.toPercentage(ratio),
                    PrintUtils.epochToHumanReadable(repairedAt),
                    PrintUtils.epochToHumanReadable(nextRepair));
        }
    }
}
