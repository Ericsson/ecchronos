/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Objects;
import java.util.UUID;

/**
 * A view representation of a scheduled repair job, containing status, progress, and configuration information.
 */
public class ScheduledRepairJobView
{
    /**
     * The possible statuses of a scheduled repair job.
     */
    public enum Status
    {
        /** Repair has completed successfully. */
        COMPLETED,
        /** Repair is running within the expected time window. */
        ON_TIME,
        /** Repair is running behind schedule. */
        LATE,
        /** Repair has exceeded the allowed time window. */
        OVERDUE,
        /** Repair is blocked and cannot proceed. */
        BLOCKED
    }

    private final UUID myNodeID;
    private final UUID myJobID;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private RepairStateSnapshot myRepairStateSnapshot;
    private final Status myStatus;
    private final double myProgress;
    private final long myNextRepair;
    private final long myCompletionTime;
    private final RepairType myRepairType;

    /**
     * Constructs a scheduled repair job view.
     *
     * @param nodeID the node identifier.
     * @param jobID the job identifier.
     * @param tableReference the table being repaired.
     * @param repairConfiguration the repair configuration for this job.
     * @param status the current status of the job.
     * @param progress the repair progress as a ratio between 0.0 and 1.0.
     * @param nextRepair the timestamp of the next scheduled repair in milliseconds since epoch.
     * @param completionTime the timestamp of the last completed repair in milliseconds since epoch.
     * @param repairType the type of repair being performed.
     */
    public ScheduledRepairJobView(final UUID nodeID, final UUID jobID, final TableReference tableReference,
            final RepairConfiguration repairConfiguration, final Status status, final double progress,
            final long nextRepair, final long completionTime, final RepairType repairType)
    {
        myNodeID = nodeID;
        myJobID = jobID;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myStatus = status;
        myProgress = progress;
        myNextRepair = nextRepair;
        myCompletionTime = completionTime;
        myRepairType = repairType;
    }

    /**
     * Constructs a scheduled repair job view with a repair state snapshot.
     *
     * @param nodeID the node identifier.
     * @param jobID the job identifier.
     * @param tableReference the table being repaired.
     * @param repairConfiguration the repair configuration for this job.
     * @param repairStateSnapshot the current repair state snapshot.
     * @param status the current status of the job.
     * @param progress the repair progress as a ratio between 0.0 and 1.0.
     * @param nextRepair the timestamp of the next scheduled repair in milliseconds since epoch.
     * @param repairType the type of repair being performed.
     */
    public ScheduledRepairJobView(final UUID nodeID, final UUID jobID, final TableReference tableReference,
            final RepairConfiguration repairConfiguration, final RepairStateSnapshot repairStateSnapshot,
            final Status status, final double progress, final long nextRepair,
            final RepairType repairType)
    {
        myNodeID = nodeID;
        myJobID = jobID;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myStatus = status;
        myProgress = progress;
        myNextRepair = nextRepair;
        myCompletionTime = repairStateSnapshot.lastCompletedAt();
        myRepairType = repairType;
    }

    /**
     * Get job ID.
     *
     * @return UUID
     */
    public UUID getJobId()
    {
        return myJobID;
    }

    /**
     * Get job ID.
     *
     * @return UUID
     */
    public UUID getNodeId()
    {
        return myNodeID;
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
     * Get repair type.
     *
     * @return RepairType
     */
    public RepairType getRepairType()
    {
        return myRepairType;
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
        else if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ScheduledRepairJobView that = (ScheduledRepairJobView) o;
        return Double.compare(that.myProgress, myProgress) == 0
                && myNextRepair == that.myNextRepair
                && Objects.equals(myJobID, that.myJobID)
                && Objects.equals(myTableReference, that.myTableReference)
                && Objects.equals(myRepairConfiguration, that.myRepairConfiguration)
                && Objects.equals(myRepairStateSnapshot, that.myRepairStateSnapshot)
                && Objects.equals(myStatus, that.myStatus)
                && Objects.equals(myRepairType, that.myRepairType);
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myJobID, myTableReference, myRepairConfiguration, myRepairStateSnapshot, myStatus, myProgress,
                myNextRepair, myRepairType);
    }
}
