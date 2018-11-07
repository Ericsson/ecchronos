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
     */
    void lastRepairedAt(TableReference tableReference, long lastRepairedAt);

    /**
     * Report the time it took to issue one repair command (session) and whether it was successful or not.
     */
    void repairTiming(TableReference tableReference, long timeTaken, TimeUnit timeUnit, boolean successful);
}
