/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AlarmPostUpdateHook implements PostUpdateHook
{
    private final RepairFaultReporter myFaultReporter;
    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final AtomicReference<Clock> myClock = new AtomicReference<>(Clock.DEFAULT);

    AlarmPostUpdateHook(TableReference tableReference, RepairConfiguration repairConfiguration, RepairFaultReporter faultReporter)
    {
        myFaultReporter = faultReporter;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;

    }
    @Override
    public void postUpdate(RepairStateSnapshot repairStateSnapshot)
    {
        long lastRepaired = repairStateSnapshot.lastRepairedAt();

        if (lastRepaired != VnodeRepairState.UNREPAIRED)
        {
            sendOrCeaseAlarm(lastRepaired);
        }
    }

    private void sendOrCeaseAlarm(long lastRepairedAt)
    {
        long msSinceLastRepair = myClock.get().getTime() - lastRepairedAt;
        RepairFaultReporter.FaultCode faultCode = null;

        if (msSinceLastRepair >= myRepairConfiguration.getRepairErrorTimeInMs())
        {
            faultCode = RepairFaultReporter.FaultCode.REPAIR_ERROR;
        }
        else if (msSinceLastRepair >= myRepairConfiguration.getRepairWarningTimeInMs())
        {
            faultCode = RepairFaultReporter.FaultCode.REPAIR_WARNING;
        }
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        data.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        if (faultCode != null)
        {
            myFaultReporter.raise(faultCode, data);
        }
        else
        {
            myFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_WARNING, data);
        }
    }

    @VisibleForTesting
    void setClock(Clock clock)
    {
        myClock.set(clock);
    }
}
