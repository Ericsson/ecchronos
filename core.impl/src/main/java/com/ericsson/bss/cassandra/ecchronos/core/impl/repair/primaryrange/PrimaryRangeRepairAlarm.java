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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.AlarmPostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raises and ceases the alarms of a primary range repaired table, based on the time since it was last repaired.
 * <p>
 * The thresholds are the ones of vnode repair: a warning alarm at {@code alarm.warn} and an error alarm at
 * {@code alarm.error}. The decision is shared with vnode repair, see {@link AlarmPostUpdateHook#faultCodeFor}.
 * <p>
 * Unlike vnode repair, which tells the reporter on every update, this reports transitions only: an alarm is raised
 * when it appears or is escalated, and ceased when it is gone. Reporters are required to handle repeated calls, so
 * this is not needed for correctness, but it keeps a table that stays late from calling the reporter on every
 * scheduler pass.
 * <p>
 * One instance is held per {@link PrimaryRangeRepairJob}, that is per node per table.
 */
final class PrimaryRangeRepairAlarm
{
    private static final Logger LOG = LoggerFactory.getLogger(PrimaryRangeRepairAlarm.class);

    /**
     * Marker value identifying the alarms raised here. A table can have a vnode schedule and a primary range
     * schedule at the same time, which would otherwise raise alarms with identical keyspace, table and node data
     * that reporters keying on the data would take for one alarm.
     */
    static final String FAULT_SOURCE_PRIMARY_RANGE_ELAPSED = "PRIMARY_RANGE_ELAPSED";

    private final RepairFaultReporter myFaultReporter;
    private final TableReference myTableReference;
    private final UUID myNodeId;
    private final RepairConfiguration myRepairConfiguration;

    /** The alarm that is currently raised, if any. */
    private Optional<RepairFaultReporter.FaultCode> myRaisedCode = Optional.empty();

    /**
     * Constructs the alarm of a table on a node.
     *
     * @param faultReporter the reporter alarms are raised and ceased in, or {@code null} for no alarms.
     * @param tableReference the table.
     * @param nodeId the node the table is repaired by.
     * @param repairConfiguration the repair configuration, with the alarm thresholds.
     */
    PrimaryRangeRepairAlarm(final RepairFaultReporter faultReporter,
            final TableReference tableReference,
            final UUID nodeId,
            final RepairConfiguration repairConfiguration)
    {
        myFaultReporter = faultReporter;
        myTableReference = tableReference;
        myNodeId = nodeId;
        myRepairConfiguration = repairConfiguration;
    }

    /**
     * Compare the time since the last repair with the thresholds, and tell the reporter if the alarm has changed.
     *
     * @param lastSuccessfulRunInMs when the table was last repaired, in epoch milliseconds.
     * @param nowInMs the current time, in epoch milliseconds.
     */
    synchronized void evaluate(final long lastSuccessfulRunInMs, final long nowInMs)
    {
        if (myFaultReporter == null)
        {
            return;
        }
        Optional<RepairFaultReporter.FaultCode> due =
                AlarmPostUpdateHook.faultCodeFor(nowInMs - lastSuccessfulRunInMs, myRepairConfiguration);

        if (due.isPresent())
        {
            if (!due.equals(myRaisedCode))
            {
                LOG.debug("{} - primary range repair alarm {} (was {})", myTableReference, due.get(), myRaisedCode);
                myFaultReporter.raise(due.get(), buildFaultData());
                myRaisedCode = due;
            }
        }
        else if (myRaisedCode.isPresent())
        {
            LOG.debug("{} - primary range repair alarm {} is gone", myTableReference, myRaisedCode);
            // Always with the warning code, like vnode repair does, also when an error was raised. Reporters are
            // known to match alarms on the data and not on the code, and a reporter written for vnode repair may
            // only look at the warning code when ceasing.
            myFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_WARNING, buildFaultData());
            myRaisedCode = Optional.empty();
        }
    }

    private Map<String, Object> buildFaultData()
    {
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_NODE_ID, myNodeId);
        data.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        data.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());
        data.put(RepairFaultReporter.FAULT_SOURCE, FAULT_SOURCE_PRIMARY_RANGE_ELAPSED);
        return data;
    }

    @VisibleForTesting
    synchronized RepairFaultReporter.FaultCode getRaisedCode()
    {
        return myRaisedCode.orElse(null);
    }
}
