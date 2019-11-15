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
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Completion;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.completers.StringsCompleter;
import org.apache.karaf.shell.support.table.ShellTable;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@Command(scope = "repair", name = "status", description = "Give the current repair status")
public class RepairStatusCommand implements Action
{
    private static final String SORT_TABLE_NAME = "TABLE_NAME";
    private static final String SORT_REPAIRED_RATIO = "REPAIRED_RATIO";
    private static final String SORT_REPAIRED_AT = "REPAIRED_AT";
    private static final String SORT_NEXT_REPAIR = "NEXT_REPAIR";

    @Reference
    private RepairScheduler myRepairScheduler;

    @Reference
    private TableRepairMetricsProvider myTableRepairMetrics;

    @Option(name = "-s", aliases = "--sort_by", description = "Sort output based on " + SORT_TABLE_NAME + "/" + SORT_REPAIRED_RATIO + "/" + SORT_REPAIRED_AT + "/" + SORT_NEXT_REPAIR, required = false, multiValued = false)
    @Completion(value = StringsCompleter.class, values = {SORT_TABLE_NAME, SORT_REPAIRED_RATIO, SORT_REPAIRED_AT, SORT_NEXT_REPAIR})
    String mySortBy = SORT_TABLE_NAME;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order", required = false, multiValued = false)
    boolean myReverse = false;

    @Option(name = "-l", aliases = "--limit", description = "Number of entries to display", required = false, multiValued = false)
    int myLimit = Integer.MAX_VALUE;

    public RepairStatusCommand()
    {
    }

    @VisibleForTesting
    RepairStatusCommand(RepairScheduler repairScheduler, TableRepairMetricsProvider tableRepairMetrics, String sortBy, boolean reverse, int limit)
    {
        myRepairScheduler = repairScheduler;
        myTableRepairMetrics = tableRepairMetrics;
        mySortBy = sortBy;
        myReverse = reverse;
        myLimit = limit;
    }

    @Override
    public Object execute() throws Exception
    {
        printTables(System.out, getOutputComparator());
        return null;
    }

    Comparator<OutputData> getOutputComparator()
    {
        Comparator<OutputData> comparator;
        switch (mySortBy)
        {
            case SORT_REPAIRED_RATIO:
                comparator = Comparator.comparing(outputData -> outputData.ratio.orElse(Double.MIN_VALUE));
                break;
            case SORT_REPAIRED_AT:
                comparator = Comparator.comparing(outputData -> outputData.repairedAt);
                break;
            case SORT_NEXT_REPAIR:
                comparator = Comparator.comparing(outputData -> outputData.nextRepair);
                break;
            case SORT_TABLE_NAME:
            default:
                comparator = Comparator.comparing(outputData -> outputData.table.toString());
                break;
        }

        return myReverse
                ? comparator.reversed()
                : comparator;
    }

    void printTables(PrintStream out, Comparator<OutputData> comparator)
    {
        ShellTable table = createShellTable();

        myRepairScheduler.getCurrentRepairJobs()
                .stream()
                .map(this::createOutputData)
                .sorted(comparator)
                .limit(myLimit)
                .forEach(outputData -> table.addRow().addContent(outputData.toRowContent()));

        table.print(out);
    }

    private ShellTable createShellTable()
    {
        ShellTable table = new ShellTable();
        table.column("Table name");
        table.column("Repaired ratio");
        table.column("Repaired at");
        table.column("Next repair");
        return table;
    }

    private OutputData createOutputData(RepairJobView job)
    {
        TableReference table = job.getTableReference();
        Optional<Double> repairRatio = myTableRepairMetrics.getRepairRatio(table);
        long repairedAt = job.getRepairStateSnapshot().lastRepairedAt();
        long nextRepair = repairedAt + job.getRepairConfiguration().getRepairIntervalInMs();

        return new OutputData(table, repairRatio, repairedAt, nextRepair);
    }

    static class OutputData
    {
        TableReference table;
        Optional<Double> ratio;
        long repairedAt;
        long nextRepair;

        OutputData(TableReference table, Optional<Double> ratio, long repairedAt, long nextRepair)
        {
            this.table = table;
            this.ratio = ratio;
            this.repairedAt = repairedAt;
            this.nextRepair = nextRepair;
        }

        List<Object> toRowContent()
        {
            return Arrays.asList(
                    table,
                    ratio.map(PrintUtils::toPercentage).orElse("-"),
                    PrintUtils.epochToHumanReadable(repairedAt),
                    PrintUtils.epochToHumanReadable(nextRepair));
        }
    }
}
