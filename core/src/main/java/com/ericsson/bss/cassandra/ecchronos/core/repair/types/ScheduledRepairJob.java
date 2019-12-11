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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;

/**
 * A representation of a scheduled repair job.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class ScheduledRepairJob
{
    public final String keyspace;
    public final String table;
    public final long repairIntervalInMs;
    public final long lastRepairedAtInMs;
    public final double repairedRatio;

    public ScheduledRepairJob(RepairJobView repairJobView)
    {
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.repairIntervalInMs = repairJobView.getRepairConfiguration().getRepairIntervalInMs();
        this.lastRepairedAtInMs = repairJobView.getRepairStateSnapshot().lastRepairedAt();

        long repairedAfter = System.currentTimeMillis() - repairIntervalInMs;

        this.repairedRatio = calculateRepaired(repairJobView.getRepairStateSnapshot().getVnodeRepairStates(), repairedAfter);
    }

    private double calculateRepaired(VnodeRepairStates vnodeRepairStates, long repairedAfter)
    {
        int nRepairedBefore = 0;
        int nRepairedAfter = 0;

        for (VnodeRepairState vnodeRepairState : vnodeRepairStates.getVnodeRepairStates())
        {
            if (vnodeRepairState.lastRepairedAt() < repairedAfter)
            {
                nRepairedBefore++;
            }
            else
            {
                nRepairedAfter++;
            }
        }

        int nTotal = nRepairedAfter + nRepairedBefore;

        if (nTotal == 0)
        {
            return 0;
        }

        return (double) nRepairedAfter / nTotal;
    }
}
