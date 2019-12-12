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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A representation of a table repair configuration.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class TableRepairConfig
{
    public final String keyspace;
    public final String table;
    public final long repairIntervalInMs;
    public final RepairParallelism repairParallelism;
    public final double repairUnwindRatio;
    public final long repairWarningTimeInMs;
    public final long repairErrorTimeInMs;

    public TableRepairConfig(RepairJobView repairJobView)
    {
        RepairConfiguration config = repairJobView.getRepairConfiguration();
        TableReference tableReference = repairJobView.getTableReference();

        this.keyspace = tableReference.getKeyspace();
        this.table = tableReference.getTable();
        this.repairIntervalInMs = config.getRepairIntervalInMs();
        this.repairParallelism = config.getRepairParallelism();
        this.repairUnwindRatio = config.getRepairUnwindRatio();
        this.repairWarningTimeInMs = config.getRepairWarningTimeInMs();
        this.repairErrorTimeInMs = config.getRepairErrorTimeInMs();
    }
}
