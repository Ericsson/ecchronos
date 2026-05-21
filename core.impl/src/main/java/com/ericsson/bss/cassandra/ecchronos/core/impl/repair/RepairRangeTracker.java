/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the outcome of individual token ranges during a repair task.
 */
public class RepairRangeTracker
{
    private final Set<LongTokenRange> myFailedRanges = ConcurrentHashMap.newKeySet();
    private final Set<LongTokenRange> mySuccessfulRanges = ConcurrentHashMap.newKeySet();

    /**
     * Record the result of a repaired range.
     *
     * @param range the token range that was repaired
     * @param repairStatus the outcome of the repair for this range
     */
    public void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        if (repairStatus.equals(RepairStatus.FAILED))
        {
            myFailedRanges.add(range);
        }
        else
        {
            mySuccessfulRanges.add(range);
        }
    }

    /**
     * @return the set of token ranges that failed during repair
     */
    public Set<LongTokenRange> getFailedRanges()
    {
        return myFailedRanges;
    }

    /**
     * @return the set of token ranges that succeeded during repair
     */
    public Set<LongTokenRange> getSuccessfulRanges()
    {
        return mySuccessfulRanges;
    }

    /**
     * @return true if any ranges have failed
     */
    public boolean hasFailedRanges()
    {
        return !myFailedRanges.isEmpty();
    }
}
