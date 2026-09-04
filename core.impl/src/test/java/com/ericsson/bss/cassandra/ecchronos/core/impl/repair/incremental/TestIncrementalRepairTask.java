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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link IncrementalRepairTask} records its execution in {@code ecchronos.repair_history} through the
 * repair-history session, and that the backward-compatible constructor performs no history tracking.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestIncrementalRepairTask
{
    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myJobId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private RepairHistory.RepairSession mySession;

    @Mock
    private Node myHistoryNode;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairHistory.newSession(any(), any(), any(), any(), any(), any())).thenReturn(mySession);
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairType(RepairType.INCREMENTAL)
                .build();
    }

    @Test
    public void testHistoryAwareTaskCreatesSession()
    {
        Set<DriverNode> participants = ImmutableSet.of(mock(DriverNode.class));

        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                myRepairHistory, myHistoryNode, myJobId, participants);

        assertThat(task).isNotNull();
        verify(myRepairHistory).newSession(eq(myHistoryNode), eq(myTableReference), eq(myJobId), any(),
                eq(participants), eq(RepairType.INCREMENTAL));
    }

    @Test
    public void testOnExecuteStartsSessionAndOnFinishFinishesSession() throws Exception
    {
        Set<DriverNode> participants = ImmutableSet.of(mock(DriverNode.class));
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                myRepairHistory, myHistoryNode, myJobId, participants);

        invokeProtected(task, "onExecute");
        verify(mySession).start();

        invokeOnFinish(task, RepairStatus.SUCCESS);
        verify(mySession).finish(RepairStatus.SUCCESS);
    }

    @Test
    public void testBackwardCompatibleConstructorDoesNotTrackHistory() throws Exception
    {
        // The legacy constructor uses a no-op session, so no interactions occur with the provided repair history.
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);

        invokeProtected(task, "onExecute");
        invokeOnFinish(task, RepairStatus.SUCCESS);

        verifyNoInteractions(myRepairHistory);
        verifyNoInteractions(mySession);
    }

    private void invokeProtected(final IncrementalRepairTask task, final String method) throws Exception
    {
        java.lang.reflect.Method m = task.getClass().getDeclaredMethod(method);
        m.setAccessible(true);
        m.invoke(task);
    }

    private void invokeOnFinish(final IncrementalRepairTask task, final RepairStatus status) throws Exception
    {
        java.lang.reflect.Method m = task.getClass().getDeclaredMethod("onFinish", RepairStatus.class);
        m.setAccessible(true);
        m.invoke(task, status);
    }
}
