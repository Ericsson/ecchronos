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

public final class RepairedAt
{
    private final long myMinRepairedAt;
    private final long myMaxRepairedAt;

    private RepairedAt(long minRepairedAt, long maxRepairedAt)
    {
        myMinRepairedAt = minRepairedAt;
        myMaxRepairedAt = maxRepairedAt;
    }

    public boolean isRepaired()
    {
        return myMinRepairedAt != Long.MAX_VALUE && myMinRepairedAt != VnodeRepairState.UNREPAIRED;
    }

    public boolean isPartiallyRepaired()
    {
        return myMinRepairedAt == VnodeRepairState.UNREPAIRED && myMaxRepairedAt != myMinRepairedAt;
    }

    public long getMaxRepairedAt()
    {
        return myMaxRepairedAt;
    }

    public long getMinRepairedAt()
    {
        return myMinRepairedAt;
    }

    @Override
    public String toString()
    {
        return String.format("(min=%d,max=%d,isRepaired=%b,isPartiallyRepaired=%b)", getMinRepairedAt(), getMaxRepairedAt(), isRepaired(), isPartiallyRepaired());
    }

    public static RepairedAt generate(VnodeRepairStates vnodeRepairStates)
    {
        long minRepairedAt = Long.MAX_VALUE;
        long maxRepairedAt = Long.MIN_VALUE;

        for (VnodeRepairState vnodeRepairState : vnodeRepairStates.getVnodeRepairStates())
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
