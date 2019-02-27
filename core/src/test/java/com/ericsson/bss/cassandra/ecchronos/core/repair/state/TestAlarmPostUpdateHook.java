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

import com.ericsson.bss.cassandra.ecchronos.core.MockedClock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestAlarmPostUpdateHook
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private RepairFaultReporter myFaultReporter;

    private MockedClock myClock = new MockedClock();
    private AlarmPostUpdateHook myPostUpdateHook;

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);

    @Before
    public void startup()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();
        myPostUpdateHook = new AlarmPostUpdateHook(myTableReference, repairConfiguration, myFaultReporter);

        myPostUpdateHook.setClock(myClock);
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify alarm ceased in preValidate
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
        reset(myFaultReporter);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatWarningAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = 2;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeased()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatErrorAlarmIsSentAndCeasedExternalRepair()
    {
        // setup - not repaired
        long daysSinceLastRepair = GC_GRACE_DAYS;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.DAYS.toMillis(daysSinceLastRepair);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - not repaired
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // setup - repaired
        lastRepaired = start;
        start = System.currentTimeMillis();

        // mock - repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - repaired
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    @Test
    public void testThatAlarmIsNotSentWhenGcGraceIsBelowRepairInterval()
    {
        // setup - not repaired
        long hoursSinceLastRepair = 23;
        long start = System.currentTimeMillis();
        long lastRepaired = start - TimeUnit.HOURS.toMillis(hoursSinceLastRepair);

        // mock - not repaired
        doReturn(lastRepaired).when(myRepairStateSnapshot).lastRepairedAt();
        myClock.setTime(start);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        // verify - not repaired
        verify(myFaultReporter, never()).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }


    @Test
    public void testLastSuccessfulRunIsBasedOnRepairHistory()
    {
        long timeOffset = TimeUnit.MINUTES.toMillis(1);
        long now = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2);
        long lastRepairedAtWarning = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS * 2);
        long lastRepairedAtAfterRepair = now - TimeUnit.DAYS.toMillis(RUN_INTERVAL_IN_DAYS) + timeOffset;

        myClock.setTime(now);

        // We have waited 2 days to repair, send alarm and run repair
        doReturn(lastRepairedAtWarning).when(myRepairStateSnapshot).lastRepairedAt();

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));
        verifyNoMoreInteractions(myFaultReporter);
        reset(myFaultReporter);

        // Repair has been completed
        doReturn(lastRepairedAtAfterRepair).when(myRepairStateSnapshot).lastRepairedAt();

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMapOf(String.class, Object.class));

        // After 10 ms we can repair again
        myClock.setTime(now + timeOffset);

        myPostUpdateHook.postUpdate(myRepairStateSnapshot);

        verify(myFaultReporter, times(0)).raise(any(RepairFaultReporter.FaultCode.class), anyMapOf(String.class, Object.class));
    }
}
