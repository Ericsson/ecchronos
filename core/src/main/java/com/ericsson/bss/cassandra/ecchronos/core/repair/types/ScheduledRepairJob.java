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

import java.util.Objects;
import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * A representation of a scheduled repair job.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class ScheduledRepairJob
{
    @NotBlank
    public String keyspace;
    @NotBlank
    public String table;
    @NotBlank
    @Min(-1)
    public long lastRepairedAtInMs;
    @NotBlank
    @Min(0)
    @Max(1)
    public double repairedRatio;
    @NotBlank
    public RepairJobView.Status status;
    @NotBlank
    @Min(-1)
    public long nextRepairInMs;
    @NotBlank
    public UUID id;
    @NotBlank
    public boolean recurring;

    public ScheduledRepairJob()
    {
    }

    @VisibleForTesting
    public ScheduledRepairJob(UUID id, String keyspace, String table, RepairJobView.Status status, double repairedRatio, long lastRepairedAtInMs, long nextRepairInMs, boolean recurring)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.status = status;
        this.repairedRatio = repairedRatio;
        this.lastRepairedAtInMs = lastRepairedAtInMs;
        this.nextRepairInMs = nextRepairInMs;
        this.recurring = recurring;
    }

    public ScheduledRepairJob(RepairJobView repairJobView)
    {
        this.id = repairJobView.getId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.lastRepairedAtInMs = repairJobView.getLastCompletedAt();
        this.nextRepairInMs = repairJobView.getNextRepair();
        this.recurring = repairJobView.isRecurring();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ScheduledRepairJob that = (ScheduledRepairJob) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs &&
                Double.compare(that.repairedRatio, repairedRatio) == 0 &&
                nextRepairInMs == that.nextRepairInMs &&
                keyspace.equals(that.keyspace) &&
                table.equals(that.table) &&
                status == that.status &&
                id.equals(that.id) &&
                recurring == that.recurring;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, keyspace, table, lastRepairedAtInMs, repairedRatio, status, nextRepairInMs, recurring);
    }
}
