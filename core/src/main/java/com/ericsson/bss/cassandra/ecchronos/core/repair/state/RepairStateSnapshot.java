/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * An immutable copy of the repair state.
 *
 * The repair state describes the current state of repairs for a table.
 * <ul>
 *     <li>When the table was last repaired - {@link #lastCompletedAt()}</li>
 *     <li>The next repair(s) to run - {@link #getRepairGroups()}</li>
 *     <li>The vnodes for the table and when they were last repaired - {@link #getVnodeRepairStates()}</li>
 *     <li>If there is a repair available - {@link #canRepair()}</li>
 * </ul>
 */
public class RepairStateSnapshot
{
    private final boolean canRepair;
    private final long myLastCompletedAt;
    private final ImmutableList<ReplicaRepairGroup> myReplicaRepairGroup;
    private final VnodeRepairStates myVnodeRepairStates;
    private final long myEstimatedRepairTime;

    private RepairStateSnapshot(Builder builder)
    {
        myLastCompletedAt = builder.myLastCompletedAt;
        myReplicaRepairGroup = builder.myReplicaRepairGroup;
        myVnodeRepairStates = builder.myVnodeRepairStates;
        myEstimatedRepairTime = myVnodeRepairStates.getRepairTime();
        canRepair = !myReplicaRepairGroup.isEmpty();
    }

    public long getRemainingRepairTime(long now, long repairIntervalMs)
    {
        long sum = 0;
        for (VnodeRepairState vnodeRepairState : myVnodeRepairStates.getVnodeRepairStates())
        {
            if(vnodeRepairState.lastRepairedAt() + (repairIntervalMs - myEstimatedRepairTime) <= now)
            {
                sum += vnodeRepairState.getRepairTime();
            }
        }
        return sum;
    }

    /**
     * Check if a repair can be performed based on the current state.
     *
     * @return True if repair can run.
     */
    public boolean canRepair()
    {
        return canRepair;
    }

    /**
     * Get the time of the last successful repair of the table.
     *
     * @return The time the table was last repaired or -1 if no information is available.
     */
    public long lastCompletedAt()
    {
        return myLastCompletedAt;
    }

    public long getEstimatedRepairTime()
    {
        return myEstimatedRepairTime;
    }

    /**
     * Information needed to run the next repair(s).
     *
     * @return The next repair(s) or an empty list if none can be run.
     */
    public List<ReplicaRepairGroup> getRepairGroups()
    {
        return myReplicaRepairGroup;
    }

    public VnodeRepairStates getVnodeRepairStates()
    {
        return myVnodeRepairStates;
    }

    @Override
    public String toString()
    {
        return "RepairStateSnapshot{" +
                "canRepair=" + canRepair +
                ", myLastCompletedAt=" + myLastCompletedAt +
                ", myReplicaRepairGroup=" + myReplicaRepairGroup +
                ", myEstimatedRepairTime=" + myEstimatedRepairTime +
                '}';
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Long myLastCompletedAt;
        private ImmutableList<ReplicaRepairGroup> myReplicaRepairGroup;
        private VnodeRepairStates myVnodeRepairStates;

        public Builder withLastCompletedAt(long lastCompletedAt)
        {
            myLastCompletedAt = lastCompletedAt;
            return this;
        }

        public Builder withReplicaRepairGroups(List<ReplicaRepairGroup> replicaRepairGroup)
        {
            myReplicaRepairGroup = ImmutableList.copyOf(replicaRepairGroup);
            return this;
        }

        public Builder withVnodeRepairStates(VnodeRepairStates vnodeRepairStates)
        {
            myVnodeRepairStates = vnodeRepairStates;
            return this;
        }

        public RepairStateSnapshot build()
        {
            return new RepairStateSnapshot(this);
        }
    }
}
