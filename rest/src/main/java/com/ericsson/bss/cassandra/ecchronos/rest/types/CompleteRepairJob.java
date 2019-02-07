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

import java.util.List;
import java.util.stream.Collectors;

/**
 * A representation of a scheduled repair job with additional virtual node state data.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class CompleteRepairJob extends ScheduledRepairJob
{
    public final List<VnodeState> vnodeStates;

    public CompleteRepairJob(String keyspace, String table, long repairIntervalInMs, long lastRepairedAt, double repaired, List<VnodeState> vnodeStates)
    {
        super(keyspace, table, repairIntervalInMs, lastRepairedAt, repaired);
        this.vnodeStates = vnodeStates;
    }

    public static CompleteRepairJob convert(RepairJobView repairJobView)
    {
        String keyspace = repairJobView.getTableReference().getKeyspace();
        String table = repairJobView.getTableReference().getTable();
        long repairIntervalInMs = repairJobView.getRepairConfiguration().getRepairIntervalInMs();
        long lastRepairedAt = repairJobView.getRepairStateSnapshot().lastRepairedAt();

        VnodeRepairStates vnodeRepairStates = repairJobView.getRepairStateSnapshot().getVnodeRepairStates();

        long repairedAfter = System.currentTimeMillis() - repairIntervalInMs;

        double repaired = calculateRepaired(vnodeRepairStates, repairedAfter);

        List<VnodeState> vnodeStates = vnodeRepairStates.getVnodeRepairStates().stream()
                .map(vrs -> VnodeState.convert(vrs, repairedAfter))
                .collect(Collectors.toList());

        return new CompleteRepairJob(keyspace, table, repairIntervalInMs, lastRepairedAt, repaired, vnodeStates);
    }

    private static double calculateRepaired(VnodeRepairStates vnodeRepairStates, long repairedAfter)
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
