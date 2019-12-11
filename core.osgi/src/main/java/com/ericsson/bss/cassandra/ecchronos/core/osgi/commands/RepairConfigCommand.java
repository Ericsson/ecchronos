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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.google.common.annotations.VisibleForTesting;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.table.ShellTable;

@Service
@Command(scope = "repair", name = "config", description = "Give the current repair configuration")
public class RepairConfigCommand implements Action
{
    @Reference
    private RepairScheduler myRepairScheduler;

    public RepairConfigCommand()
    {
    }

    @VisibleForTesting
    RepairConfigCommand(RepairScheduler repairScheduler)
    {
        myRepairScheduler = repairScheduler;
    }

    @Override
    public Object execute() throws Exception
    {
        printConfig(System.out);
        return null;
    }

    void printConfig(PrintStream out)
    {
        ShellTable table = createShellTable();

        myRepairScheduler.getCurrentRepairJobs()
                .stream()
                .sorted(Comparator.comparing(job -> job.getTableReference().toString()))
                .forEach(job -> table.addRow().addContent(createRowContent(job)));

        table.print(out);
    }

    private ShellTable createShellTable()
    {
        ShellTable table = new ShellTable();
        table.column("Table name");
        table.column("Interval");
        table.column("Parallelism");
        table.column("Unwind ratio");
        table.column("Warning time");
        table.column("Error time");
        return table;
    }

    private List<Object> createRowContent(RepairJobView job)
    {
        RepairConfiguration config = job.getRepairConfiguration();
        return Arrays.asList(
                job.getTableReference().toString(),
                PrintUtils.durationToHumanReadable(config.getRepairIntervalInMs()),
                config.getRepairParallelism(),
                PrintUtils.toPercentage(config.getRepairUnwindRatio()),
                PrintUtils.durationToHumanReadable(config.getRepairWarningTimeInMs()),
                PrintUtils.durationToHumanReadable(config.getRepairErrorTimeInMs())
                );
    }
}
