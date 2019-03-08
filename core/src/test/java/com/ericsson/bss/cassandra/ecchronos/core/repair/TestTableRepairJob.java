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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
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
import com.datastax.driver.core.exceptions.OverloadedException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;

@RunWith (MockitoJUnitRunner.class)
public class TestTableRepairJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long GC_GRACE_DAYS = 10;

    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private Metadata myMetadata;

    @Mock
    private LockFactory myLockFactory;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private RepairState myRepairState;

    @Mock
    private RepairStateSnapshot myRepairStateSnapshot;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private TableRepairJob myRepairJob;

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);

    @Before
    public void startup()
    {
        doReturn(-1L).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(myRepairStateSnapshot).when(myRepairState).getSnapshot();

        doNothing().when(myRepairState).update();

        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairWarningTime(RUN_INTERVAL_IN_DAYS * 2, TimeUnit.DAYS)
                .withRepairErrorTime(GC_GRACE_DAYS, TimeUnit.DAYS)
                .build();

        myRepairJob = new TableRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRepairState(myRepairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(repairConfiguration)
                .withRepairLockType(RepairLockType.VNODE)
                .build();
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myJmxProxyFactory));
        verifyNoMoreInteractions(ignoreStubs(myMetadata));
        verifyNoMoreInteractions(ignoreStubs(myLockFactory));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myRepairState));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testPrevalidateNotRepairable()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairState, times(1)).update();
        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNeedRepair()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairState, times(1)).update();
        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateNotRepairableThenRepairable()
    {
        // mock
        doReturn(false).doReturn(true).when(myRepairStateSnapshot).canRepair();

        assertThat(myRepairJob.runnable()).isFalse();
        assertThat(myRepairJob.runnable()).isTrue();

        verify(myRepairState, times(2)).update();
        verify(myRepairStateSnapshot, times(2)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsOverloadException()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        doThrow(new OverloadedException(null, "Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPrevalidateUpdateThrowsException()
    {
        // mock
        doReturn(false).when(myRepairStateSnapshot).canRepair();
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        assertThat(myRepairJob.runnable()).isFalse();

        verify(myRepairStateSnapshot, times(1)).canRepair();
    }

    @Test
    public void testPostExecuteRepaired()
    {
        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteRepairedWithFailure()
    {
        // mock
        long repairedAt = System.currentTimeMillis();
        doReturn(repairedAt).when(myRepairStateSnapshot).lastRepairedAt();
        doReturn(false).when(myRepairStateSnapshot).canRepair();

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(repairedAt);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepaired()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteNotRepairedWithFailure()
    {
        // mock
        doReturn(true).when(myRepairStateSnapshot).canRepair();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(false);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
        verify(myRepairState, times(1)).update();
    }

    @Test
    public void testPostExecuteUpdateThrowsException()
    {
        // mock
        doThrow(new RuntimeException("Expected exception")).when(myRepairState).update();

        long lastRun = myRepairJob.getLastSuccessfulRun();

        myRepairJob.postExecute(true);

        assertThat(myRepairJob.getLastSuccessfulRun()).isEqualTo(lastRun);
    }
}
