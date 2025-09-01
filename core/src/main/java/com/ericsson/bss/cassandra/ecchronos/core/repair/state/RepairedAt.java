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

import java.util.Collection;

/**
 * Utility class to determine collective repaired at information for {@link VnodeRepairStates}.
 *
 * A value of {@link Long#MAX_VALUE} indicates that no repair information is available.
 * A value of {@link VnodeRepairState#UNREPAIRED} indicates that the status is unknown.
 */
public record RepairedAt(long minRepairedAt, long maxRepairedAt)
{
    /**
     * Check if all vnodes have repaired at information.
     *
     * @return True if all vnodes have repaired at information.
     */
    public boolean isRepaired()
    {
        return minRepairedAt != Long.MAX_VALUE && minRepairedAt != VnodeRepairState.UNREPAIRED;
    }

    /**
     * Check if only some vnodes have repaired at information.
     *
     * @return True if some vnodes have been repaired but not all.
     */
    public boolean isPartiallyRepaired()
    {
        return minRepairedAt == VnodeRepairState.UNREPAIRED && maxRepairedAt != minRepairedAt;
    }

    @Override
    public String toString()
    {
        return String.format("(min=%d,max=%d,isRepaired=%b,isPartiallyRepaired=%b)",
                minRepairedAt, maxRepairedAt, isRepaired(), isPartiallyRepaired());
    }

    /**
     * Generate a repaired at.
     *
     * @param vnodeRepairStates Vnode repair states.
     * @return RepairedAt
     */
    public static RepairedAt generate(final VnodeRepairStates vnodeRepairStates)
    {
        return RepairedAt.generate(vnodeRepairStates.getVnodeRepairStates());
    }

    public static RepairedAt generate(final Collection<VnodeRepairState> vnodeRepairStates)
    {
        long minRepairedAt = Long.MAX_VALUE;
        long maxRepairedAt = Long.MIN_VALUE;

        for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
        {
            long repairedAt = vnodeRepairState.lastRepairedAt();

            if (repairedAt > maxRepairedAt)
            {
                maxRepairedAt = repairedAt;
            }

            if (repairedAt < minRepairedAt)
            {
                minRepairedAt = repairedAt;
            }
        }

        return new RepairedAt(minRepairedAt, maxRepairedAt);
    }
}
