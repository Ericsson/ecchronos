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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.Objects;
import java.util.UUID;

/**
 * Object representing a single schedule (only used for holding the data)
 */
public class ScheduleView
{
    private final UUID myId;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairStateSnapshot myRepairStateSnapshot;
    private final ScheduleView.Status myStatus;
    private final double myProgress;
    private final long myLastRepair;
    private final long myNextRepair;

    public enum Status
    {
        WAITING, RUNNING, WARNING, ERROR, COMPLETED
    }

    public ScheduleView(UUID id, TableReference tableReference, RepairConfiguration repairConfiguration,
            RepairStateSnapshot repairStateSnapshot, ScheduleView.Status status, double progress, long lastRepair,
            long nextRepair)
    {
        myId = id;
        myRepairConfiguration = repairConfiguration;
        myRepairStateSnapshot = repairStateSnapshot;
        myTableReference = tableReference;
        myStatus = status;
        myProgress = progress;
        myLastRepair = lastRepair;
        myNextRepair = nextRepair;
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
    public ScheduleView.Status getStatus()
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
     * @return When last repair job completed
     */
    public long getLastRepair()
    {
        return myLastRepair;
    }

    /**
     * @return By when the next repair will run
     */
    public long getNextRepair()
    {
        return myNextRepair;
    }

    @Override
    public boolean equals(Object o)
    {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;
        ScheduleView that = (ScheduleView) o;
        return Objects.equals(myId, that.myId) &&
                Objects.equals(myTableReference, that.myTableReference) &&
                Objects.equals(myRepairConfiguration, that.myRepairConfiguration) &&
                Objects.equals(myRepairStateSnapshot, that.myRepairStateSnapshot) &&
                Objects.equals(myStatus, that.myStatus) &&
                Objects.equals(myProgress, that.myProgress) &&
                myLastRepair == that.myLastRepair && myNextRepair == that.myNextRepair;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myId, myTableReference, myRepairConfiguration, myRepairStateSnapshot, myStatus, myProgress, myLastRepair, myNextRepair);
    }

    @Override
    public String toString()
    {
        return "ScheduleView{" +
                "myId=" + myId +
                ", myTableReference=" + myTableReference +
                ", myRepairConfiguration=" + myRepairConfiguration +
                ", myRepairStateSnapshot=" + myRepairStateSnapshot +
                ", myStatus=" + myStatus +
                ", myProgress=" + myProgress +
                ", myLastRepair=" + myLastRepair +
                ", myNextRepair=" + myNextRepair +
                '}';
    }
}
