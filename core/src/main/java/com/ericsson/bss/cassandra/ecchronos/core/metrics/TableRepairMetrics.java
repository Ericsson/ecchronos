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

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Interface for reporting table based repair metrics.
 */
public interface TableRepairMetrics
{
    /**
     * Report number of repaired/not repaired ranges for the provided table.
     *
     * @param tableReference The table
     * @param repairedRanges The number of repaired ranges
     * @param notRepairedRanges The number of not repaired ranges
     */
    void repairState(TableReference tableReference, int repairedRanges, int notRepairedRanges);

    /**
     * Report the time the table was last repaired.
     *
     * @param tableReference The table to update the last repaired at value for.
     * @param lastRepairedAt The last time the table was repaired.
     */
    void lastRepairedAt(TableReference tableReference, long lastRepairedAt);

    /**
     * Report the remaining repair time for table.
     * @param tableReference The table to update the remaining repair time for.
     * @param remainingRepairTime The remaining time to fully repair the table.
     */
    void remainingRepairTime(TableReference tableReference, long remainingRepairTime);

    /**
     * Report the time it took to issue one repair command (session) and whether it was successful or not.
     *
     * @param tableReference The table the repair was performed on.
     * @param timeTaken The time it took to perform the repair.
     * @param timeUnit The {@link TimeUnit} used for the time taken.
     * @param successful If the repair was successful or not.
     */
    void repairTiming(TableReference tableReference, long timeTaken, TimeUnit timeUnit, boolean successful);
}
