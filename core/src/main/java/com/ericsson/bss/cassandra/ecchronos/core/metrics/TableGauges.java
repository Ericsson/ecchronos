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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holder class for repair metrics gauges for a specific table.
 */
public class TableGauges implements Closeable
{
    private final AtomicReference<Double> myRepairRatio = new AtomicReference<>(0.0);
    private final AtomicReference<Double> myDataRepairRatio = new AtomicReference<>(0.0);
    private final AtomicReference<Long> myLastRepairedAt = new AtomicReference<>(0L);
    private final AtomicReference<Long> myRemainingRepairTime = new AtomicReference<>(0L);

    private final TableStorageStates myTableStorageStates;
    private final TableReference myTableReference;

    public TableGauges(final TableReference tableReference, final TableStorageStates tableStorageStates)
    {
        myTableReference = tableReference;
        myTableStorageStates = tableStorageStates;
    }

    /**
     * Update repair ratio.
     *
     * @param repairedRanges Ranges repaired
     * @param notRepairedRanges Ranges NOT repaired
     */
    void repairRatio(final int repairedRanges, final int notRepairedRanges)
    {
        double repairRatio = 0.0;
        int allRanges = repairedRanges + notRepairedRanges;
        if (allRanges > 0)
        {
            repairRatio = (double) repairedRanges / (repairedRanges + notRepairedRanges);
        }
        myRepairRatio.set(repairRatio);
    }

    /**
     * Get repair ratio of ranges.
     *
     * @return ratio of repaired ranges
     */
    Double getRepairRatio()
    {
        return myRepairRatio.get();
    }

    /**
     * Update repair ratio of data.
     */
    void dataRepairRatio()
    {
        double dataRepairRatio = 1.0; // 100% when no data to repair
        long totalDataSize = myTableStorageStates.getDataSize(myTableReference);
        if (totalDataSize != 0)
        {
            double repairedDataSize = myRepairRatio.get() * totalDataSize;
            dataRepairRatio = repairedDataSize / totalDataSize;
        }
        myDataRepairRatio.set(dataRepairRatio);
    }

    /**
     * Get repair ratio of data.
     *
     * @return ratio of repaired data
     */
    Double getDataRepairRatio()
    {
        return myDataRepairRatio.get();
    }

    /**
     * Update last repaired at.
     *
     * @param lastRepairedAt Last repaired at
     */
    void lastRepairedAt(final long lastRepairedAt)
    {
        myLastRepairedAt.set(lastRepairedAt);
    }

    /**
     * Get last repaired at.
     *
     * @return last repaired at
     */
    long getLastRepairedAt()
    {
        return myLastRepairedAt.get();
    }

    /**
     * Update remaining repair time.
     *
     * @param remainingRepairTime Remaining repair time
     */
    public void remainingRepairTime(final long remainingRepairTime)
    {
        myRemainingRepairTime.set(remainingRepairTime);
    }

    /**
     * Get remaining repair time.
     *
     * @return remaining repair time
     */
    long getRemainingRepairTime()
    {
        return myRemainingRepairTime.get();
    }

    @Override
    public void close()
    {
    }
}
