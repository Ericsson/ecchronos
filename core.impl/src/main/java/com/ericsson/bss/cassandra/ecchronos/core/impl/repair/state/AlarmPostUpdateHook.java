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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.PostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A update hook that raises and ceases alarms based on the elapsed time without repair.
 */
public class AlarmPostUpdateHook implements PostUpdateHook
{
    private final RepairFaultReporter myFaultReporter;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final AtomicReference<Clock> myClock = new AtomicReference<>(Clock.systemDefaultZone());

    /**
     * Constructs an AlarmPostUpdateHook.
     *
     * @param tableReference the table reference for alarm identification.
     * @param repairConfiguration the repair configuration with alarm thresholds.
     * @param faultReporter the fault reporter used to raise and cease alarms.
     */
    public AlarmPostUpdateHook(final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final RepairFaultReporter faultReporter)
    {
        myFaultReporter = faultReporter;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;

    }

    /**
     * Post update.
     *
     * @param repairStateSnapshot The current repair state snapshot.
     * @param hostId The host identifier of the node being updated.
     */
    @Override
    public void postUpdate(final RepairStateSnapshot repairStateSnapshot, final UUID hostId)
    {
        long lastRepaired = repairStateSnapshot.lastCompletedAt();

        if (lastRepaired != VnodeRepairState.UNREPAIRED)
        {
            sendOrCeaseAlarm(lastRepaired, hostId);
        }
    }

    private void sendOrCeaseAlarm(final long lastRepairedAt, final UUID hostId)
    {
        long msSinceLastRepair = myClock.get().millis() - lastRepairedAt;
        Optional<RepairFaultReporter.FaultCode> faultCode = faultCodeFor(msSinceLastRepair, myRepairConfiguration);
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_NODE_ID, hostId);
        data.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        data.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        if (faultCode.isPresent())
        {
            myFaultReporter.raise(faultCode.get(), data);
        }
        else
        {
            myFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_WARNING, data);
        }
    }

    /**
     * Decides which alarm, if any, a table calls for when it was last repaired the given time ago. The error alarm
     * is raised at {@code alarm.error} and the warning alarm at {@code alarm.warn}, which are also the times a
     * schedule is reported as overdue and late. It is shared by every repair type that raises time based alarms, so
     * that they all use the same thresholds.
     *
     * @param msSinceLastRepair the time since the last repair, in milliseconds.
     * @param repairConfiguration the repair configuration with the alarm thresholds.
     * @return the alarm to raise, or empty if the table is repaired often enough.
     */
    public static Optional<RepairFaultReporter.FaultCode> faultCodeFor(final long msSinceLastRepair,
            final RepairConfiguration repairConfiguration)
    {
        Optional<RepairFaultReporter.FaultCode> faultCode = Optional.empty();

        if (msSinceLastRepair >= repairConfiguration.getRepairErrorTimeInMs())
        {
            faultCode = Optional.of(RepairFaultReporter.FaultCode.REPAIR_ERROR);
        }
        else if (msSinceLastRepair >= repairConfiguration.getRepairWarningTimeInMs())
        {
            faultCode = Optional.of(RepairFaultReporter.FaultCode.REPAIR_WARNING);
        }
        return faultCode;
    }

    /**
     * Set clock.
     *
     * @param clock the clock to use for time calculations.
     */
    @VisibleForTesting
    void setClock(final Clock clock)
    {
        myClock.set(clock);
    }
}

