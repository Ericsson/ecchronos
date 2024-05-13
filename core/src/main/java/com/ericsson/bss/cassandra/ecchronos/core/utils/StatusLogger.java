/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StatusLogger
{

    private StatusLogger()
    {
        throw new AssertionError("Utility classes should not be instantiated");
    }
    private static final Logger LOG = LoggerFactory.getLogger(StatusLogger.class);

    static final String NODE_REPAIR_SESSIONS = "node.repair.sessions";

    static final String NODE_REMAINING_REPAIR_TIME = "node.remaining.repair.time";

    static final String NODE_TIME_SINCE_LAST_REPAIRED = "node.time.since.last.repaired";

    static final String NODE_REPAIRED_RATIO = "node.repaired.ratio";


/*
Log the state of the table, if repairs for a table failed for more than time defined
in configuration in ecc.yml
//

 */
    public static void log(final MeterRegistry myMeterRegistry)
    {
        Timer failedRepairSessions = myMeterRegistry.find(NODE_REPAIR_SESSIONS)
                .tags("successful", "false")
                .timer();
        if (failedRepairSessions != null)
        {
            LOG.debug("Total repair failures in node till now is: {}", failedRepairSessions.count());
        }

        Timer successfulRepairSessions = myMeterRegistry.find(NODE_REPAIR_SESSIONS)
                .tags("successful", "true")
                .timer();
        if (successfulRepairSessions != null)
        {
            LOG.debug("Total repair success in node till now is:{}", successfulRepairSessions.count());
        }

        Gauge nodeTimeSinceLastRepaired = myMeterRegistry.find(NODE_TIME_SINCE_LAST_REPAIRED)
                .gauge();

        if (nodeTimeSinceLastRepaired != null)
        {
            LOG.debug("Node last repaired at: {}", nodeTimeSinceLastRepaired.value());
        }

        Gauge nodeRemainingRepairTime = myMeterRegistry.find(NODE_REMAINING_REPAIR_TIME)
                .gauge();
        if (nodeRemainingRepairTime != null)
        {
            LOG.debug("Remaining time for node repair :{}", nodeRemainingRepairTime.value());
        }

        Gauge nodeRepairedRatio = myMeterRegistry.find(NODE_REPAIRED_RATIO)
                .gauge();
        if (nodeRepairedRatio != null)
        {
            LOG.debug("Node Repair Ratio is :: {}", nodeRepairedRatio.value());
        }

    }

}
