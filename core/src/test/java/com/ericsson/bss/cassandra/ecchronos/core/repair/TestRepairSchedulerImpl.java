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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

@RunWith (MockitoJUnitRunner.class)
public class TestRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = new TableReference("keyspace", "table");

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private RepairFaultReporter myFaultReporter;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairStateFactory myRepairStateFactory;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Before
    public void init()
    {
        doReturn(myRepairStateSnapshot).when(myRepairState).getSnapshot();
        doReturn(myRepairState).when(myRepairStateFactory).create(eq(TABLE_REFERENCE), any());
    }

    @Test
    public void testConfigureNewTable()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRemoveTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.removeConfiguration(TABLE_REFERENCE);
        verify(scheduleManager, timeout(1000)).deschedule(any(ScheduledJob.class));

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfiguration()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        long expectedUpdatedRepairInterval = TimeUnit.DAYS.toMillis(1);

        RepairConfiguration updatedRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(expectedUpdatedRepairInterval, TimeUnit.MILLISECONDS)
                .build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, updatedRepairConfiguration);

        verify(scheduleManager, timeout(1000).times(2)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, timeout(1000)).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(updatedRepairConfiguration));
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testUpdateTableConfigurationToSame()
    {
        RepairSchedulerImpl repairSchedulerImpl = defaultRepairSchedulerImplBuilder().build();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));
        verify(scheduleManager, never()).deschedule(any(ScheduledJob.class));
        verify(myRepairStateFactory).create(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairState, atLeastOnce()).update();

        repairSchedulerImpl.putConfiguration(TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairSchedulerImpl.close();
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(myRepairStateFactory);
        verifyNoMoreInteractions(scheduleManager);
    }

    private RepairSchedulerImpl.Builder defaultRepairSchedulerImplBuilder()
    {
        return RepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withFaultReporter(myFaultReporter)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withRepairStateFactory(myRepairStateFactory)
                .withRepairLockType(RepairLockType.VNODE);
    }
}
