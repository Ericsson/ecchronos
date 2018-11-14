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

/**
 * An immutable copy of the repair state.
 */
public class RepairStateSnapshot
{
    private final boolean canRepair;
    private final long myLastRepairedAt;
    private final ReplicaRepairGroup myReplicaRepairGroup;
    private final VnodeRepairStates myVnodeRepairStates;

    private RepairStateSnapshot(Builder builder)
    {
        canRepair = builder.canRepair;
        myLastRepairedAt = builder.myLastRepairedAt;
        myReplicaRepairGroup = builder.myReplicaRepairGroup;
        myVnodeRepairStates = builder.myVnodeRepairStates;
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

    public ReplicaRepairGroup getRepairGroup()
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
        private Boolean canRepair;
        private Long myLastRepairedAt;
        private ReplicaRepairGroup myReplicaRepairGroup;
        private VnodeRepairStates myVnodeRepairStates;

        public Builder canRepair(boolean canRepair)
        {
            this.canRepair = canRepair;
            return this;
        }

        public Builder withLastRepairedAt(long lastRepairedAt)
        {
            myLastRepairedAt = lastRepairedAt;
            return this;
        }

        public Builder withReplicaRepairGroup(ReplicaRepairGroup replicaRepairGroup)
        {
            myReplicaRepairGroup = replicaRepairGroup;
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
