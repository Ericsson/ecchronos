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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

@RunWith (MockitoJUnitRunner.class)
public class TestTableRepairJobIntegration
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final Map<String, String> keyspaceReplication;

    static
    {
        keyspaceReplication = new HashMap<>();

        keyspaceReplication.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        keyspaceReplication.put("dc1", "3");
    }

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private RunPolicy myRunPolicy;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private Metadata myMetadata;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableMetadata myTableMetadata;

    @Mock
    private TableOptionsMetadata myTableOptionsMetadata;

    @Mock
    private RepairFaultReporter myFaultReporter;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private TableStorageStates myTableStorageStates;

    @Mock
    private Clock myClock;

    private TableRepairJob myTableRepairJob;

    private ScheduleManagerImpl myScheduler;

    @Before
    public void setup()
    {
        initKeyspaceMetadata();

        doNothing().when(myRepairState).update();
        doReturn(myRepairStateSnapshot).when(myRepairState).getSnapshot();

        doReturn(System.currentTimeMillis()).when(myClock).getTime();

        doReturn(true).when(myRepairStateSnapshot).canRepair();
        myScheduler = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .build();
        myScheduler.addRunPolicy(job -> myRunPolicy.validate(job));

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(1, TimeUnit.DAYS)
                .build();

        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(2, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .build();

        myTableRepairJob = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(new TableReference(keyspaceName, tableName))
                .withRepairState(myRepairState)
                .withFaultReporter(myFaultReporter)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .withRepairLockType(RepairLockType.VNODE)
                .withTableStorageStates(myTableStorageStates)
                .build();

        myTableRepairJob.setClock(myClock);

        myScheduler.schedule(myTableRepairJob);
    }

    @After
    public void finalizeTest()
    {
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myRunPolicy));
        verifyNoMoreInteractions(ignoreStubs(myRepairState));
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myMetadata));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myTableMetadata));
        verifyNoMoreInteractions(ignoreStubs(myTableOptionsMetadata));
        verifyNoMoreInteractions(ignoreStubs(myFaultReporter));
        verifyNoMoreInteractions(ignoreStubs(myClock));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testWarningAlarm()
    {
        // setup
        doReturn(1L).when(myRunPolicy).validate(eq(myTableRepairJob));

        long start = System.currentTimeMillis();
        long lastRepairedWarning = start - TimeUnit.DAYS.toMillis(2);
        long lastRepairedError = start - TimeUnit.DAYS.toMillis(10);

        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put(RepairFaultReporter.FAULT_KEYSPACE, keyspaceName);
        expectedData.put(RepairFaultReporter.FAULT_TABLE, tableName);

        // mock
        doReturn(start).when(myClock).getTime();
        doReturn(lastRepairedWarning).when(myRepairStateSnapshot).lastRepairedAt();

        // Run warning
        myScheduler.run();
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));

        // Run error
        doReturn(lastRepairedError).when(myRepairStateSnapshot).lastRepairedAt();

        myScheduler.run();
        verify(myFaultReporter).raise(eq(RepairFaultReporter.FaultCode.REPAIR_ERROR), eq(expectedData));

        // Run clear

        doReturn(start).when(myRepairStateSnapshot).lastRepairedAt();

        myScheduler.run();
        verify(myFaultReporter).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), eq(expectedData));
    }

    private void initKeyspaceMetadata()
    {
        doReturn((int) TimeUnit.DAYS.toSeconds(10)).when(myTableOptionsMetadata).getGcGraceInSeconds();
        doReturn(myTableOptionsMetadata).when(myTableMetadata).getOptions();
        doReturn(myTableMetadata).when(myKeyspaceMetadata).getTable(eq(tableName));
        doReturn(myKeyspaceMetadata).when(myMetadata).getKeyspace(eq(keyspaceName));

        doReturn(keyspaceReplication).when(myKeyspaceMetadata).getReplication();
    }
}
