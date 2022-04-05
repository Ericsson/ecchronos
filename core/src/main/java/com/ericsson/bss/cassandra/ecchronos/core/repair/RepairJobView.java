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

import java.util.Objects;
import java.util.UUID;

/**
 * Read only view of a scheduled repair job.
 */
public abstract class RepairJobView
{
    private final UUID myId;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;
    private final Status myStatus;
    private final double myProgress;

    public enum Status
    {
        COMPLETED, IN_QUEUE, WARNING, ERROR, BLOCKED
    }

    public RepairJobView(UUID id, TableReference tableReference, RepairConfiguration repairConfiguration, RepairStateSnapshot repairStateSnapshot, Status status, double progress)
    {
        myId = id;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myStatus = status;
        myProgress = progress;
    }

    /**
     * @return the unique identifier for this job.
     */
    public UUID getId()
    {
        return myId;
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

    /**
     * @return the status of this job
     */
    public Status getStatus()
    {
        return myStatus;
    }

    /**
     * @return the progress of this job
     */
    public double getProgress()
    {
        return myProgress;
    }

    /**
     * @return When repair job was last repaired
     */
    public abstract long getLastCompletedAt();

    /**
     * @return By when the next repair will run
     */
    public abstract long getNextRepair();

    /**
     * @return if repair job is on ondemand or recurring
     */
    public abstract Boolean isRecurring();


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairJobView that = (RepairJobView) o;
        return Objects.equals(myId, that.myId) &&
                Objects.equals(myTableReference, that.myTableReference) &&
                Objects.equals(myRepairConfiguration, that.myRepairConfiguration) &&
                Objects.equals(myRepairStateSnapshot, that.myRepairStateSnapshot) &&
                Objects.equals(myStatus, that.myStatus) &&
                Objects.equals(myProgress, that.myProgress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(myId, myTableReference, myRepairConfiguration, myRepairStateSnapshot, myStatus, myProgress);
    }
}
