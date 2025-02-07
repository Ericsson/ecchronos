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

import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an on-demand repair job view.
 * This class encapsulates the status and progress of an on-demand repair job,
 * along with related metadata like the host ID, completion time, and repair type.
 */
public class OnDemandRepairJobView
{
    /**
     * Enum representing the possible statuses of an on-demand repair job.
     */
    public enum Status
    {
        /**
         * Represents a completed repair job.
         */
        COMPLETED,

        /**
         * Represents a repair job that is currently in the queue and waiting to be processed.
         */
        IN_QUEUE,

        /**
         * Represents a repair job that has encountered a warning during execution.
         */
        WARNING,

        /**
         * Represents a repair job that has encountered an error during execution.
         */
        ERROR,

        /**
         * Represents a repair job that is blocked and cannot proceed.
         */
        BLOCKED
    }

    private final UUID myJobId;
    private final TableReference myTableReference;
    private final Status myStatus;
    private final double myProgress;
    private final UUID myNodeId;
    private final long myCompletionTime;
    private final RepairType myRepairType;

    /**
     * Constructor for OnDemandRepairJobView.
     *
     * @param jobId The UUID representing the repair job ID.
     * @param hostId The UUID representing the host ID associated with the repair.
     * @param tableReference The table reference for the repair job.
     * @param status The status of the repair job.
     * @param progress The progress of the repair job, between 0.0 and 1.0.
     * @param completionTime The completion time of the repair job in milliseconds.
     * @param repairType The type of repair being performed.
     */
    public OnDemandRepairJobView(final UUID jobId,
                                 final UUID hostId,
                                 final TableReference tableReference,
                                 final Status status,
                                 final double progress,
                                 final long completionTime,
                                 final RepairType repairType)
    {
        myJobId = jobId;
        myTableReference = tableReference;
        myStatus = status;
        myProgress = progress;
        myNodeId = hostId;
        myCompletionTime = completionTime;
        myRepairType = repairType;
    }

    /**
     * Get id.
     *
     * @return UUID
     */
    public UUID getJobId()
    {
        return myJobId;
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
     * Get host id.
     *
     * @return UUID
     */
    public UUID getNodeId()
    {
        return myNodeId;
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
     * Equality check.
     *
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
        OnDemandRepairJobView that = (OnDemandRepairJobView) o;
        return Double.compare(that.myProgress, myProgress) == 0 && myCompletionTime == that.myCompletionTime
                && Objects.equals(myJobId, that.myJobId) && Objects.equals(myTableReference, that.myTableReference)
                && myStatus == that.myStatus && Objects.equals(myNodeId, that.myNodeId)
                && Objects.equals(myRepairType, that.myRepairType);
    }

    /**
     * Hash representation of the object.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myJobId, myNodeId, myTableReference, myStatus, myProgress, myNodeId, myCompletionTime, myRepairType);
    }
}
