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

import java.util.Objects;
import java.util.UUID;

public class ScheduledRepairJobView
{
    public enum Status
    {
        COMPLETED, ON_TIME, LATE, OVERDUE, BLOCKED
    }

    private final UUID myId;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;
    private final Status myStatus;
    private final double myProgress;
    private final long myNextRepair;
    private final long myCompletionTime;

    public ScheduledRepairJobView(final UUID id,
                                  final TableReference tableReference,
                                  final RepairConfiguration repairConfiguration,
                                  final RepairStateSnapshot repairStateSnapshot,
                                  final Status status,
                                  final double progress,
                                  final long nextRepair)
    {
        myId = id;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myStatus = status;
        myProgress = progress;
        myNextRepair = nextRepair;
        myCompletionTime = repairStateSnapshot.lastCompletedAt();
    }

    /**
     * Get id.
     *
     * @return UUID
     */
    public UUID getId()
    {
        return myId;
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get repair configuration.
     *
     * @return RepairConfiguration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    /**
     * Get repair snapshot.
     *
     * @return RepairStateSnapshot
     */
    public RepairStateSnapshot getRepairStateSnapshot()
    {
        return myRepairStateSnapshot;
    }

    /**
     * Get status.
     *
     * @return Status
     */
    public Status getStatus()
    {
        return myStatus;
    }

    /**
     * Get progress.
     *
     * @return double
     */
    public double getProgress()
    {
        return myProgress;
    }

    /**
     * Get next repair.
     *
     * @return long
     */
    public long getNextRepair()
    {
        return myNextRepair;
    }

    /**
     * Get completion time.
     *
     * @return long
     */
    public long getCompletionTime()
    {
        return myCompletionTime;
    }

    /**
     * Equality (completion time is not considered).
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
        ScheduledRepairJobView that = (ScheduledRepairJobView) o;
        return Double.compare(that.myProgress, myProgress) == 0
                && myNextRepair == that.myNextRepair
                && Objects.equals(myId, that.myId)
                && Objects.equals(myTableReference, that.myTableReference)
                && Objects.equals(myRepairConfiguration, that.myRepairConfiguration)
                && Objects.equals(myRepairStateSnapshot, that.myRepairStateSnapshot)
                && myStatus == that.myStatus;
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myId, myTableReference, myRepairConfiguration, myRepairStateSnapshot, myStatus, myProgress,
                myNextRepair);
    }
}
