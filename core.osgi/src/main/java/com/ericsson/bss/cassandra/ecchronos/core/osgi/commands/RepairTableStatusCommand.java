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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Completion;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.CommandException;
import org.apache.karaf.shell.support.completers.StringsCompleter;
import org.apache.karaf.shell.support.table.ShellTable;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@Service
@Command(scope = "repair", name = "table-status", description = "Give the current repair status for the given table")
public class RepairTableStatusCommand implements Action
{
    private static final String SORT_RANGE = "RANGE";
    private static final String SORT_REPAIRED_AT = "REPAIRED_AT";

    @Reference
    private RepairScheduler myRepairSchedulerService;

    @Option(name = "-t", aliases = "--table_reference", description = "The table reference in format <keyspace>.<table>", required = true, multiValued = false)
    @Completion(TableReferenceCompleter.class)
    String myTableRef;

    @Option(name = "-s", aliases = "--sort_by", description = "Sort output based on " + SORT_RANGE + "/"  + SORT_REPAIRED_AT, required = false, multiValued = false)
    @Completion(value = StringsCompleter.class, values = {SORT_RANGE, SORT_REPAIRED_AT})
    String mySortBy = SORT_RANGE;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order", required = false, multiValued = false)
    boolean myReverse = false;

    @Option(name = "-l", aliases = "--limit", description = "Number of entries to display", required = false, multiValued = false)
    int myLimit = Integer.MAX_VALUE;

    public RepairTableStatusCommand()
    {
    }

    @VisibleForTesting
    RepairTableStatusCommand(RepairScheduler repairScheduler, String tableRef, String sortBy, boolean reverse, int limit)
    {
        myRepairSchedulerService = repairScheduler;
        myTableRef = tableRef;
        mySortBy = sortBy;
        myReverse = reverse;
        myLimit = limit;
    }

    @Override
    public Object execute() throws Exception
    {
        printTableRanges(System.out, getVnodeRepairStates(), getRepairStateComparator());
        return null;
    }

    VnodeRepairStates getVnodeRepairStates() throws CommandException
    {
        return myRepairSchedulerService.getCurrentRepairJobs()
                .stream()
                .filter(repairJobView -> repairJobView.getTableReference().toString().equalsIgnoreCase(myTableRef))
                .findFirst()
                .map(RepairJobView::getRepairStateSnapshot)
                .map(RepairStateSnapshot::getVnodeRepairStates)
                .orElseThrow(() -> new CommandException("Table reference '" + myTableRef + "' was not found. Format must be <keyspace>.<table>"));
    }

    Comparator<VnodeRepairState> getRepairStateComparator()
    {
        Comparator<VnodeRepairState> comparator;
        switch (mySortBy)
        {
            case SORT_REPAIRED_AT:
                comparator = Comparator.comparing(vnodeRepairState -> vnodeRepairState.lastRepairedAt());
                break;
            case SORT_RANGE:
            default:
                comparator = Comparator.comparing(vnodeRepairState -> vnodeRepairState.getTokenRange().start);
                break;
        }

        return myReverse
                ? comparator.reversed()
                : comparator;
    }

    void printTableRanges(PrintStream out, VnodeRepairStates repairStates, Comparator<VnodeRepairState> comparator)
    {
        ShellTable table = new ShellTable();
        table.column("Range");
        table.column("Last repaired at");
        table.column("Replicas");

        repairStates.getVnodeRepairStates()
                .stream()
                .sorted(comparator)
                .limit(myLimit)
                .forEach(state -> table.addRow().addContent(getRowContent(state)));

        table.print(out);
    }

    private static List<Object> getRowContent(VnodeRepairState state)
    {
        LongTokenRange tokenRange = state.getTokenRange();
        String lastRepairedAt = PrintUtils.epochToHumanReadable(state.lastRepairedAt());
        ImmutableSet<Host> replicas = state.getReplicas();
        return Arrays.asList(tokenRange, lastRepairedAt, replicas);
    }
}
