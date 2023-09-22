/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class VnodeRepairStateUtils
{
    private VnodeRepairStateUtils()
    {
        // Static
    }

    /**
     * Get estimated repair time for the vnodes. This is based on the last repair that was run for the vnodes.
     * @param vnodeRepairStates The vnodes.
     * @return The estimated repair time in ms.
     */
    public static long getRepairTime(final Collection<VnodeRepairState> vnodeRepairStates)
    {
        Set<VnodeRepairState> vnodes = groupByStartedAt(vnodeRepairStates);
        long sum = 0;
        for (VnodeRepairState vnode : vnodes)
        {
            sum += vnode.getRepairTime();
        }
        return sum;
    }

    /**
     * Calculate the remaining repair time for the vnodes.
     * @param vnodeRepairStates The vnodes.
     * @param repairIntervalMs The repair interval.
     * @param now The time now.
     * @param totalRepairTime The estimated repair time for the vnodes.
     * @return The remaining repair time for the vnodes.
     */
    public static long getRemainingRepairTime(final Collection<VnodeRepairState> vnodeRepairStates,
            final long repairIntervalMs, final long now, final long totalRepairTime)
    {
        Set<VnodeRepairState> vnodes = groupByStartedAt(vnodeRepairStates);
        long sum = 0;
        for (VnodeRepairState vnodeRepairState : vnodes)
        {
            if (vnodeRepairState.lastRepairedAt() + (repairIntervalMs - totalRepairTime) <= now)
            {
                sum += vnodeRepairState.getRepairTime();
            }
        }
        return sum;
    }

    /**
     * Group vnodes by startedAt timestamp.
     * @param vnodeRepairStates The vnodes.
     * @return A single vnode per startedAt.
     */
    private static Set<VnodeRepairState> groupByStartedAt(final Collection<VnodeRepairState> vnodeRepairStates)
    {
        Map<Long, Set<VnodeRepairState>> vnodesByStartedAt = new HashMap<>();
        for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
        {
            long startedAt = vnodeRepairState.getStartedAt();
            Set<VnodeRepairState> vnodes = vnodesByStartedAt.getOrDefault(startedAt, new HashSet<>());
            vnodes.add(vnodeRepairState);
            vnodesByStartedAt.put(startedAt, vnodes);
        }
        Set<VnodeRepairState> reducedVnodes = new HashSet<>();
        for (Set<VnodeRepairState> vnodes : vnodesByStartedAt.values())
        {
            for (VnodeRepairState vnodeRepairState : vnodes)
            {
                reducedVnodes.add(vnodeRepairState);
                break;
            }
        }
        return reducedVnodes;
    }
}
