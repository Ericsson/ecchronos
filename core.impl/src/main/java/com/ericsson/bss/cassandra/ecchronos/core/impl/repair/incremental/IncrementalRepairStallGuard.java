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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-cycle stall guard for incremental repair (issue #1812, Layer B).
 * <p>
 * This is a monitoring backstop that runs independently of the per-repair confirmation in
 * {@link IncrementalRepairTask}. It tracks the {@code repaired_at} / {@code percentRepaired} signal for a single
 * table across repair cycles. When a table repeatedly reports successful incremental repairs but its repaired state
 * never advances — while there is still pending/unrepaired data — it raises a {@link RepairFaultReporter.FaultCode}
 * warning so operators are alerted that repairs appear to be succeeding without making progress.
 * <p>
 * The guard distinguishes the two situations that both leave {@code repaired_at} unchanged:
 * <ul>
 *     <li>"Nothing to repair" (steady state, {@code percentRepaired == 100}) — not a fault; the alarm is ceased.</li>
 *     <li>"Claimed success, had pending data, but repaired state did not advance" — the stall condition; after a
 *     configurable number of consecutive occurrences the warning is raised.</li>
 * </ul>
 * A single instance is held per {@link IncrementalRepairJob} (one job per node per table) and is invoked once per
 * successful repair cycle, so its state naturally accumulates across cycles.
 */
public final class IncrementalRepairStallGuard
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalRepairStallGuard.class);

    /** Default number of consecutive stalled cycles before the warning is raised. */
    static final int DEFAULT_STALL_THRESHOLD = 3;

    /** Marker value identifying alarms raised by this guard, so they do not collide with other sources. */
    private static final String FAULT_SOURCE_INCREMENTAL_STALL = "INCREMENTAL_STALL";

    private static final double PERCENT_FULLY_REPAIRED = 100.0d;

    private final RepairFaultReporter myFaultReporter;
    private final CassandraMetrics myCassandraMetrics;
    private final TableReference myTableReference;
    private final UUID myNodeId;
    private final int myStallThreshold;

    private long myLastMaxRepairedAt = Long.MIN_VALUE;
    private double myLastPercentRepaired = -1.0d;
    private int myConsecutiveStalledCycles = 0;
    private boolean myAlarmRaised = false;

    /**
     * Constructs a stall guard with the default threshold.
     *
     * @param faultReporter the fault reporter used to raise/cease the warning.
     * @param cassandraMetrics the metrics source for repaired state.
     * @param tableReference the table being monitored.
     * @param nodeId the node whose metrics are read.
     */
    public IncrementalRepairStallGuard(final RepairFaultReporter faultReporter,
            final CassandraMetrics cassandraMetrics,
            final TableReference tableReference,
            final UUID nodeId)
    {
        this(faultReporter, cassandraMetrics, tableReference, nodeId, DEFAULT_STALL_THRESHOLD);
    }

    /**
     * Constructs a stall guard with an explicit threshold.
     *
     * @param faultReporter the fault reporter used to raise/cease the warning.
     * @param cassandraMetrics the metrics source for repaired state.
     * @param tableReference the table being monitored.
     * @param nodeId the node whose metrics are read.
     * @param stallThreshold the number of consecutive stalled cycles before the warning is raised. Must be > 0.
     */
    @VisibleForTesting
    IncrementalRepairStallGuard(final RepairFaultReporter faultReporter,
            final CassandraMetrics cassandraMetrics,
            final TableReference tableReference,
            final UUID nodeId,
            final int stallThreshold)
    {
        myFaultReporter = faultReporter;
        myCassandraMetrics = cassandraMetrics;
        myTableReference = tableReference;
        myNodeId = nodeId;
        myStallThreshold = stallThreshold;
    }

    /**
     * Record a completed incremental repair cycle that reported success and update the stall state.
     * <p>
     * Should be called once per successful repair cycle.
     */
    public synchronized void onRepairCycleCompleted()
    {
        if (myFaultReporter == null || myCassandraMetrics == null)
        {
            return;
        }

        myCassandraMetrics.forceRefresh(myNodeId, myTableReference);
        long currentMaxRepairedAt = myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference);
        double currentPercentRepaired = myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference);

        boolean firstObservation = myLastPercentRepaired < 0;
        boolean fullyRepaired = currentPercentRepaired >= PERCENT_FULLY_REPAIRED;
        boolean advanced = currentMaxRepairedAt > myLastMaxRepairedAt
                || currentPercentRepaired > myLastPercentRepaired;

        if (fullyRepaired || firstObservation || advanced)
        {
            // Making progress, fully repaired, or we have no baseline yet — not a stall.
            resetAndCease();
        }
        else
        {
            // Reported success, still has pending data, and repaired state did not move since the previous cycle.
            myConsecutiveStalledCycles++;
            LOG.warn("{} - incremental repair reported success but repaired state has not advanced for {} "
                            + "consecutive cycle(s) (percentRepaired={}, maxRepairedAt={})",
                    myTableReference, myConsecutiveStalledCycles, currentPercentRepaired, currentMaxRepairedAt);
            if (myConsecutiveStalledCycles >= myStallThreshold && !myAlarmRaised)
            {
                myFaultReporter.raise(RepairFaultReporter.FaultCode.REPAIR_WARNING, buildFaultData());
                myAlarmRaised = true;
            }
        }

        myLastMaxRepairedAt = currentMaxRepairedAt;
        myLastPercentRepaired = currentPercentRepaired;
    }

    private void resetAndCease()
    {
        myConsecutiveStalledCycles = 0;
        if (myAlarmRaised)
        {
            myFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_WARNING, buildFaultData());
            myAlarmRaised = false;
        }
    }

    private Map<String, Object> buildFaultData()
    {
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_NODE_ID, myNodeId);
        data.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        data.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());
        // Distinguish this alarm from the time-based REPAIR_WARNING raised by AlarmPostUpdateHook for the same
        // {node, keyspace, table}; without this marker reporters that key alarms on the data map would conflate them.
        data.put(RepairFaultReporter.FAULT_SOURCE, FAULT_SOURCE_INCREMENTAL_STALL);
        return data;
    }

    @VisibleForTesting
    int getConsecutiveStalledCycles()
    {
        return myConsecutiveStalledCycles;
    }

    @VisibleForTesting
    boolean isAlarmRaised()
    {
        return myAlarmRaised;
    }
}
