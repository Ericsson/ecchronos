/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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
import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;
import java.util.UUID;


/**
 * A representation of an on demand repair.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class OnDemandRepair
{
    public UUID id;
    public String keyspace;
    public String table;
    public RepairJobView.Status status;
    public double repairedRatio;
    public long completedAt;

    public OnDemandRepair()
    {
    }

    @VisibleForTesting
    public OnDemandRepair(UUID id, String keyspace, String table, RepairJobView.Status status, double repairedRatio, long completedAt)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.status = status;
        this.repairedRatio = repairedRatio;
        this.completedAt = completedAt;
    }

    public OnDemandRepair(RepairJobView repairJobView)
    {
        this.id = repairJobView.getId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.completedAt = repairJobView.getLastCompletedAt();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        OnDemandRepair that = (OnDemandRepair) o;
        return  Double.compare(that.repairedRatio, repairedRatio) == 0 &&
                keyspace.equals(that.keyspace) &&
                table.equals(that.table) &&
                status == that.status &&
                id.equals(that.id) &&
                completedAt == that.completedAt;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, keyspace, table, repairedRatio, status, completedAt);
    }
}
