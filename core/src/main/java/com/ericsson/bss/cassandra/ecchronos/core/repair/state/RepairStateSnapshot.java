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
 *     <li>When the table was last repaired - {@link #lastRepairedAt()}</li>
 *     <li>The next repair(s) to run - {@link #getRepairGroups()}</li>
 *     <li>The vnodes for the table and when they were last repaired - {@link #getVnodeRepairStates()}</li>
 *     <li>If there is a repair available - {@link #canRepair()}</li>
 * </ul>
 */
public class RepairStateSnapshot
{
    private final boolean canRepair;
    private final long myLastRepairedAt;
    private final ImmutableList<ReplicaRepairGroup> myReplicaRepairGroup;
    private final VnodeRepairStates myVnodeRepairStates;

    private RepairStateSnapshot(Builder builder)
    {
        myLastRepairedAt = builder.myLastRepairedAt;
        myReplicaRepairGroup = builder.myReplicaRepairGroup;
        myVnodeRepairStates = builder.myVnodeRepairStates;

        canRepair = !myReplicaRepairGroup.isEmpty();
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
    public long lastRepairedAt()
    {
        return myLastRepairedAt;
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
        return String.format("(canRepair=%b,lastRepaired=%d,replicaRepairGroup=%s)", canRepair, myLastRepairedAt, myReplicaRepairGroup);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Long myLastRepairedAt;
        private ImmutableList<ReplicaRepairGroup> myReplicaRepairGroup;
        private VnodeRepairStates myVnodeRepairStates;

        public Builder withLastRepairedAt(long lastRepairedAt)
        {
            myLastRepairedAt = lastRepairedAt;
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
