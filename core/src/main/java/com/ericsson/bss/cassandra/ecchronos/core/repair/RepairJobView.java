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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Read only view of a scheduled repair job.
 */
public class RepairJobView
{
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;

    public RepairJobView(TableReference tableReference, RepairConfiguration repairConfiguration, RepairStateSnapshot repairStateSnapshot)
    {
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
    }

    /**
     * @return the table this job is scheduled for.
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * @return the repair configuration used.
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    /**
     * @return a snapshot of the current repair state.
     */
    public RepairStateSnapshot getRepairStateSnapshot()
    {
        return myRepairStateSnapshot;
    }
}
