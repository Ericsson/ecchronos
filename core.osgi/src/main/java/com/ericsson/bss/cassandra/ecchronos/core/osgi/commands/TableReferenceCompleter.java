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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.api.console.CommandLine;
import org.apache.karaf.shell.api.console.Completer;
import org.apache.karaf.shell.api.console.Session;
import org.apache.karaf.shell.support.completers.StringsCompleter;

import java.util.List;

@Service
public class TableReferenceCompleter implements Completer
{
    @Reference
    private RepairScheduler myRepairSchedulerService;

    @Override
    public final int complete(final Session session, final CommandLine commandLine, final List<String> candidates)
    {
        StringsCompleter delegate = new StringsCompleter();
        try
        {
            myRepairSchedulerService.getCurrentRepairJobs()
                    .stream()
                    .map(RepairJobView::getTableReference)
                    .forEach(tableReference -> delegate.getStrings().add(tableReference.toString()));
        }
        catch (Exception e) // NOPMD
        {
            // Ignore
        }
        return delegate.complete(session, commandLine, candidates);
    }
}
