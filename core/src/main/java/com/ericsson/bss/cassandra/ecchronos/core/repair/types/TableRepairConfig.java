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

import java.util.Objects;
import java.util.UUID;

/**
 * A representation of a table repair configuration.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class TableRepairConfig
{
    public UUID id;
    public String keyspace;
    public String table;
    public long repairIntervalInMs;
    public RepairParallelism repairParallelism;
    public double repairUnwindRatio;
    public long repairWarningTimeInMs;
    public long repairErrorTimeInMs;

    public TableRepairConfig()
    {
    }

    public TableRepairConfig(RepairJobView repairJobView)
    {
        RepairConfiguration config = repairJobView.getRepairConfiguration();
        TableReference tableReference = repairJobView.getTableReference();

        this.id = repairJobView.getId();
        this.keyspace = tableReference.getKeyspace();
        this.table = tableReference.getTable();
        this.repairIntervalInMs = config.getRepairIntervalInMs();
        this.repairParallelism = config.getRepairParallelism();
        this.repairUnwindRatio = config.getRepairUnwindRatio();
        this.repairWarningTimeInMs = config.getRepairWarningTimeInMs();
        this.repairErrorTimeInMs = config.getRepairErrorTimeInMs();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TableRepairConfig that = (TableRepairConfig) o;
        return Objects.equals(id, that.id) &&
                repairIntervalInMs == that.repairIntervalInMs &&
                Double.compare(that.repairUnwindRatio, repairUnwindRatio) == 0 &&
                repairWarningTimeInMs == that.repairWarningTimeInMs &&
                repairErrorTimeInMs == that.repairErrorTimeInMs &&
                Objects.equals(keyspace, that.keyspace) &&
                Objects.equals(table, that.table) &&
                repairParallelism == that.repairParallelism;
    }

    @Override
    public int hashCode()
    {
        return Objects
                .hash(id, keyspace, table, repairIntervalInMs, repairParallelism, repairUnwindRatio, repairWarningTimeInMs,
                        repairErrorTimeInMs);
    }
}
