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
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A representation of a scheduled repair job with additional virtual node state data.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class CompleteRepairJob extends ScheduledRepairJob
{
    public List<VirtualNodeState> virtualNodeStates;

    public CompleteRepairJob()
    {
        super();
    }

    public CompleteRepairJob(RepairJobView repairJobView)
    {
        super(repairJobView);

        if (repairJobView.getRepairStateSnapshot() == null)
        {
            virtualNodeStates = new ArrayList<>();
            return;
        }
        long repairedAfter = System.currentTimeMillis() - repairJobView.getRepairConfiguration().getRepairIntervalInMs();
        VnodeRepairStates vnodeRepairStates = repairJobView.getRepairStateSnapshot().getVnodeRepairStates();

        this.virtualNodeStates = vnodeRepairStates.getVnodeRepairStates().stream()
                .map(vrs -> VirtualNodeState.convert(vrs, repairedAfter))
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        CompleteRepairJob that = (CompleteRepairJob) o;
        return virtualNodeStates.equals(that.virtualNodeStates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), virtualNodeStates);
    }
}
