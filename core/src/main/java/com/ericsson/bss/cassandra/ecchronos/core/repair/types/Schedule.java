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

import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduleView;
import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;
import java.util.UUID;

/**
 * A representation of a schedule.
 *
 * Used to have a type to convert to JSON.
 */
public class Schedule
{
    public final String keyspace;
    public final String table;
    public final double repairedRatio;
    public final ScheduleView.Status status;
    public final long lastRepairedAtInMs;
    public final long nextRepairInMs;
    public final UUID id;

    @VisibleForTesting
    public Schedule(UUID id, String keyspace, String table, ScheduleView.Status status, double repairedRatio,
            long lastRepairedAtInMs, long nextRepairInMs)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.status = status;
        this.repairedRatio = repairedRatio;
        this.lastRepairedAtInMs = lastRepairedAtInMs;
        this.nextRepairInMs = nextRepairInMs;
    }

    public Schedule(ScheduleView repairJobView)
    {
        this.id = repairJobView.getId();
        this.keyspace = repairJobView.getTableReference().getKeyspace();
        this.table = repairJobView.getTableReference().getTable();
        this.status = repairJobView.getStatus();
        this.repairedRatio = repairJobView.getProgress();
        this.lastRepairedAtInMs = repairJobView.getLastRepair();
        this.nextRepairInMs = repairJobView.getNextRepair();
    }

    @Override
    public boolean equals(Object o)
    {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;
        Schedule that = (Schedule) o;
        return lastRepairedAtInMs == that.lastRepairedAtInMs &&
                Double.compare(that.repairedRatio, repairedRatio) == 0 &&
                nextRepairInMs == that.nextRepairInMs &&
                keyspace.equals(that.keyspace) &&
                table.equals(that.table) &&
                status == that.status &&
                id.equals(that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, keyspace, table, lastRepairedAtInMs, repairedRatio, status, nextRepairInMs);
    }
}
