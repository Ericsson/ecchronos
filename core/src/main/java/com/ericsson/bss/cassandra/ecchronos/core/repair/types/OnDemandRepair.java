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

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.Objects;
import java.util.UUID;


/**
 * A representation of an on demand repair.
 *
 * Primarily used to have a type to convert to JSON.
 */
@SuppressWarnings("VisibilityModifier")
public class OnDemandRepair
{
    @NotBlank
    public UUID id;
    @NotBlank
    public UUID hostId;
    @NotBlank
    public String keyspace;
    @NotBlank
    public String table;
    @NotBlank
    public OnDemandRepairJobView.Status status;
    @NotBlank
    @Min(0)
    @Max(1)
    public double repairedRatio;
    @NotBlank
    @Min(-1)
    public long completedAt;

    public OnDemandRepair()
    {
    }

    @VisibleForTesting
    public OnDemandRepair(final UUID theId,
                          final UUID theHostId,
                          final String theKeyspace,
                          final String theTable,
                          final OnDemandRepairJobView.Status theStatus,
                          final double theRepairedRatio,
                          final long wasCompletedAt)
    {
        this.id = theId;
        this.hostId = theHostId;
        this.keyspace = theKeyspace;
        this.table = theTable;
        this.status = theStatus;
        this.repairedRatio = theRepairedRatio;
        this.completedAt = wasCompletedAt;
    }


    public OnDemandRepair(final OnDemandRepairJobView repairJobView)
    {
        this.id = repairJobView.getId();
        this.hostId = repairJobView.getHostId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.completedAt = repairJobView.getCompletionTime();
    }

    /**
     * Equality.
     *
     * @param o The object to compare to.
     * @return boolean
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        OnDemandRepair that = (OnDemandRepair) o;
        return  id.equals(that.id)
                && hostId.equals(that.hostId)
                && keyspace.equals(that.keyspace)
                && table.equals(that.table)
                && status == that.status
                && Double.compare(that.repairedRatio, repairedRatio) == 0
                && completedAt == that.completedAt;
    }

    /**
     * Hash code representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(id, hostId, keyspace, table, repairedRatio, status, completedAt);
    }
}
