/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.UUID;

public class ScheduledRepairJobView extends RepairJobView
{
    private final long myNextRepair;

    public ScheduledRepairJobView(UUID id, TableReference tableReference, RepairConfiguration repairConfiguration,
            RepairStateSnapshot repairStateSnapshot, Status status, double progress, long nextRepair)
    {
        super(id, tableReference, repairConfiguration, repairStateSnapshot, status, progress);
        myNextRepair = nextRepair;
    }

    @Override
    public long getLastCompletedAt()
    {
        return getRepairStateSnapshot().lastCompletedAt();
    }

    @Override
    public long getNextRepair()
    {
        return myNextRepair;
    }

    @Override
    public Boolean isRecurring()
    {
        return true;
    }

    @Override
    public UUID getHostId()
    {
        return null;
    }
}
