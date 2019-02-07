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
package com.ericsson.bss.cassandra.ecchronos.rest.types;

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
    public final long lastRepairedAt;
    public final double repaired;

    public ScheduledRepairJob(String keyspace, String table, long repairIntervalInMs, long lastRepairedAt, double repaired)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.repairIntervalInMs = repairIntervalInMs;
        this.lastRepairedAt = lastRepairedAt;
        this.repaired = repaired;
    }

    public static ScheduledRepairJob convert(RepairJobView repairJobView)
    {
        String keyspace = repairJobView.getTableReference().getKeyspace();
        String table = repairJobView.getTableReference().getTable();
        long repairIntervalInMs = repairJobView.getRepairConfiguration().getRepairIntervalInMs();
        long lastRepairedAt = repairJobView.getRepairStateSnapshot().lastRepairedAt();

        double repaired = calculateRepaired(repairJobView.getRepairStateSnapshot().getVnodeRepairStates(), repairIntervalInMs);

        return new ScheduledRepairJob(keyspace, table, repairIntervalInMs, lastRepairedAt, repaired);
    }

    private static double calculateRepaired(VnodeRepairStates vnodeRepairStates, long repairIntervalInMs)
    {
        long repairedAfter = System.currentTimeMillis() - repairIntervalInMs;

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
