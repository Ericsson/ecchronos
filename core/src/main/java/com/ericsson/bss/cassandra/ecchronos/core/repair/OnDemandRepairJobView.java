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

import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

public class OnDemandRepairJobView extends RepairJobView
{
    UUID hostId;
    long completionTime;

    public OnDemandRepairJobView(UUID id, UUID hostId, TableReference tableReference, RepairConfiguration repairConfiguration, Status status, double progress, long completionTime)
    {
        super(id, tableReference, repairConfiguration, null, status, progress);
        this.hostId = hostId;
        this.completionTime = completionTime;
    }

    @Override
    public UUID getHostId()
    {
        return this.hostId;
    }

    @Override
    public long getLastCompletedAt()
    {
        return completionTime;
    }

    @Override
    public long getNextRepair()
    {
        return -1;
    }

    @Override
    public Boolean isRecurring()
    {
        return false;
    }
}
