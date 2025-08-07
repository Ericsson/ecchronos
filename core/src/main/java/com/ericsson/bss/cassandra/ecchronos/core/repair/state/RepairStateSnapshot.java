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
 *     <li>The next repair(s) to run - {@link #repairGroups()}</li>
 *     <li>The vnodes for the table and when they were last repaired - {@link #vnodeRepairStates()}</li>
 *     <li>If there is a repair available - {@link #canRepair()}</li>
 * </ul>
 */
public record RepairStateSnapshot(long lastCompletedAt, long createdAt, List<ReplicaRepairGroup> repairGroups,
                                  VnodeRepairStates vnodeRepairStates, long estimatedRepairTime, boolean canRepair)
{
    public long getRemainingRepairTime(final long now, final long repairIntervalMs)
    {
        return VnodeRepairStateUtils.getRemainingRepairTime(vnodeRepairStates.getVnodeRepairStates(),
                repairIntervalMs, now, estimatedRepairTime);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Long myLastCompletedAt;
        private long myCreatedAt = System.currentTimeMillis();
        private List<ReplicaRepairGroup> myReplicaRepairGroup;
        private VnodeRepairStates myVnodeRepairStates;

        /**
         * Build repair state snapshot with last completed at.
         *
         * @param lastCompletedAt Time stamp of last completion.
         * @return Builder
         */
        public Builder withLastCompletedAt(final long lastCompletedAt)
        {
            myLastCompletedAt = lastCompletedAt;
            return this;
        }

        /**
         * Build repair state snapshot with replica repair groups.
         *
         * @param replicaRepairGroup The repair replica group.
         * @return Builder
         */
        public Builder withReplicaRepairGroups(final List<ReplicaRepairGroup> replicaRepairGroup)
        {
            myReplicaRepairGroup = ImmutableList.copyOf(replicaRepairGroup);
            return this;
        }

        /**
         * Build repair state snapshot with vNode repair state.
         *
         * @param vnodeRepairStates The vnode repair states.
         * @return Builder
         */
        public Builder withVnodeRepairStates(final VnodeRepairStates vnodeRepairStates)
        {
            myVnodeRepairStates = vnodeRepairStates;
            return this;
        }

        /**
         * Build repair state snapshot with created at timestamp.
         *
         * @param createdAt The created at timestamp.
         * @return Builder
         */
        public Builder withCreatedAt(final long createdAt)
        {
            myCreatedAt = createdAt;
            return this;
        }

        /**
         * Build repair state snapshot.
         *
         * @return RepairStateSnapshot
         */
        public RepairStateSnapshot build()
        {
            return new RepairStateSnapshot(myLastCompletedAt,
                    myCreatedAt,
                    myReplicaRepairGroup,
                    myVnodeRepairStates,
                    VnodeRepairStateUtils.getRepairTime(myVnodeRepairStates.getVnodeRepairStates()),
                    !myReplicaRepairGroup.isEmpty());
        }
    }
}
