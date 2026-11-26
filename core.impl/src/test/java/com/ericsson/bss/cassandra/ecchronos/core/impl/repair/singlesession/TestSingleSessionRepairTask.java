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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.singlesession;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.TransientRepairException;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.management.NotificationListener;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestSingleSessionRepairTask
{
    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myJobId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private DistributedJmxProxy myJmxProxy;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private RepairHistory.RepairSession mySession;
    @Mock
    private Node myHistoryNode;

    private RepairConfiguration myRepairConfiguration;
    private Set<DriverNode> myParticipants;

    @Before
    public void setup() throws Exception
    {
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
        when(myJmxProxy.addStorageServiceListener(any(UUID.class), any(NotificationListener.class))).thenReturn(true);
        when(myRepairHistory.newSession(any(), any(), any(), any(), any(), any())).thenReturn(mySession);
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairType(RepairType.PRIMARY_RANGE)
                .build();
        myParticipants = ImmutableSet.of(mock(DriverNode.class));
    }

    private SingleSessionRepairTask newTask()
    {
        return newTask(RepairType.PRIMARY_RANGE);
    }

    private SingleSessionRepairTask newTask(final RepairType repairType)
    {
        return new SingleSessionRepairTask(repairType, myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration,
                myTableRepairMetrics, myRepairHistory, myHistoryNode, myJobId, myParticipants);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> optionsOf(final SingleSessionRepairTask task) throws Exception
    {
        Method getOptions = SingleSessionRepairTask.class.getDeclaredMethod("getOptions");
        getOptions.setAccessible(true);
        return (Map<String, String>) getOptions.invoke(task);
    }

    @Test
    public void testOptionsRepairAllPrimaryRangesInAFullRepair() throws Exception
    {
        Map<String, String> options = optionsOf(newTask());

        assertThat(options)
                .containsEntry(RepairOptions.PRIMARY_RANGE_KEY, "true")
                .containsEntry(RepairOptions.INCREMENTAL_KEY, "false")
                .containsEntry(RepairOptions.COLUMNFAMILIES_KEY, "table")
                .containsEntry(RepairOptions.PARALLELISM_KEY, RepairParallelism.PARALLEL.getName())
                .containsEntry(RepairOptions.UNREPLICATED_KEY, "true");
    }

    @Test
    public void testOptionsHaveNoRangesAndNoHosts() throws Exception
    {
        // Cassandra rejects a primary range repair that is combined with ranges or hosts.
        Map<String, String> options = optionsOf(newTask());

        assertThat(options).doesNotContainKeys(RepairOptions.RANGES_KEY, RepairOptions.HOSTS_KEY);
    }

    @Test
    public void testAllReplicatedRangesIsNotAPrimaryRangeRepair() throws Exception
    {
        Map<String, String> options = optionsOf(newTask(RepairType.ALL_REPLICATED_RANGES));

        assertThat(options)
                .containsEntry(RepairOptions.PRIMARY_RANGE_KEY, "false")
                .containsEntry(RepairOptions.INCREMENTAL_KEY, "false")
                .containsEntry(RepairOptions.COLUMNFAMILIES_KEY, "table")
                .containsEntry(RepairOptions.PARALLELISM_KEY, RepairParallelism.PARALLEL.getName())
                .containsEntry(RepairOptions.UNREPLICATED_KEY, "true");
    }

    @Test
    public void testAllReplicatedRangesHasNoRangesAndNoHosts() throws Exception
    {
        // Without them Cassandra repairs the ranges of the node, with the replicas in all datacenters.
        Map<String, String> options = optionsOf(newTask(RepairType.ALL_REPLICATED_RANGES));

        assertThat(options).doesNotContainKeys(RepairOptions.RANGES_KEY, RepairOptions.HOSTS_KEY);
    }

    @Test
    public void testAllReplicatedRangesSessionHasItsOwnType()
    {
        newTask(RepairType.ALL_REPLICATED_RANGES);

        verify(myRepairHistory).newSession(eq(myHistoryNode), eq(myTableReference), eq(myJobId),
                eq(LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE)), eq(myParticipants),
                eq(RepairType.ALL_REPLICATED_RANGES));
    }

    @Test
    public void testOnlyThePrimaryRangeTypeIsPrimaryRange() throws Exception
    {
        assertThat(optionsOf(newTask(RepairType.PRIMARY_RANGE)))
                .containsEntry(RepairOptions.PRIMARY_RANGE_KEY, "true");
        assertThat(optionsOf(newTask(RepairType.ALL_REPLICATED_RANGES)))
                .containsEntry(RepairOptions.PRIMARY_RANGE_KEY, "false");
    }

    @Test
    public void testTheOtherRepairTypesAreRejected()
    {
        for (RepairType repairType : new RepairType[] {RepairType.VNODE, RepairType.PARALLEL_VNODE, RepairType.INCREMENTAL})
        {
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> newTask(repairType));
        }
    }

    @Test
    public void testSessionIsTypedPrimaryRangeAndKeyedByTheHistoryNode()
    {
        // The session type is given by the task, whatever repair type the configuration says.
        myRepairConfiguration = RepairConfiguration.newBuilder().withRepairType(RepairType.VNODE).build();

        newTask();

        verify(myRepairHistory).newSession(eq(myHistoryNode), eq(myTableReference), eq(myJobId),
                eq(LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE)), eq(myParticipants),
                eq(RepairType.PRIMARY_RANGE));
    }

    @Test
    public void testOnExecuteStartsAndOnFinishFinishesTheSession() throws Exception
    {
        SingleSessionRepairTask task = newTask();

        invoke(task, "onExecute");
        verify(mySession).start();

        invokeOnFinish(task, RepairStatus.SUCCESS);
        verify(mySession).finish(RepairStatus.SUCCESS);
    }

    @Test
    public void testRepairIsStartedWithThePrimaryRangeOptionsOnTheNodeOfTheTask() throws Exception
    {
        when(myJmxProxy.repairAsync(eq(myNodeId), anyString(), any())).thenReturn(0);

        assertThatExceptionOfType(TransientRepairException.class).isThrownBy(newTask()::execute);

        ArgumentCaptor<Map<String, String>> options = ArgumentCaptor.forClass(Map.class);
        verify(myJmxProxy).repairAsync(eq(myNodeId), eq("keyspace"), options.capture());
        assertThat(options.getValue()).containsEntry(RepairOptions.PRIMARY_RANGE_KEY, "true")
                .doesNotContainKeys(RepairOptions.RANGES_KEY, RepairOptions.HOSTS_KEY);
    }

    @Test
    public void testRepairThatDoesNotStartFinishesTheSessionAsFailed() throws Exception
    {
        when(myJmxProxy.repairAsync(eq(myNodeId), anyString(), any())).thenReturn(0);

        SingleSessionRepairTask task = newTask();

        assertThatExceptionOfType(TransientRepairException.class).isThrownBy(task::execute);
        verify(mySession).start();
        verify(mySession).finish(RepairStatus.FAILED);
        verify(mySession, never()).finish(RepairStatus.SUCCESS);
    }

    @Test
    public void testFailedRangesFailTheTask() throws Exception
    {
        SingleSessionRepairTask task = newTask();
        invoke(task, "onRangeFinished", new Class<?>[] {LongTokenRange.class, RepairStatus.class},
                LongTokenRange.of(1, 100), RepairStatus.FAILED);

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(() -> invokeVerify(task))
                .withMessageContaining("failed ranges");
    }

    @Test
    public void testWithoutFailedRangesVerificationPasses() throws Exception
    {
        invokeVerify(newTask());
    }

    private static Object invoke(final SingleSessionRepairTask task, final String method) throws Exception
    {
        return invoke(task, method, new Class<?>[0]);
    }

    /** Invokes a protected method, looking it up on the class that declares it. */
    private static Object invoke(final SingleSessionRepairTask task, final String method, final Class<?>[] types,
            final Object... args) throws Exception
    {
        Class<?> declaring = SingleSessionRepairTask.class;
        while (true)
        {
            try
            {
                Method m = declaring.getDeclaredMethod(method, types);
                m.setAccessible(true);
                return m.invoke(task, args);
            }
            catch (NoSuchMethodException e)
            {
                declaring = declaring.getSuperclass();
                if (declaring == null)
                {
                    throw e;
                }
            }
        }
    }

    private static void invokeOnFinish(final SingleSessionRepairTask task, final RepairStatus status) throws Exception
    {
        invoke(task, "onFinish", new Class<?>[] {RepairStatus.class}, status);
    }

    private void invokeVerify(final SingleSessionRepairTask task) throws Exception
    {
        try
        {
            invoke(task, "verifyRepair", new Class<?>[] {DistributedJmxProxy.class}, myJmxProxy);
        }
        catch (java.lang.reflect.InvocationTargetException e)
        {
            if (e.getCause() instanceof Exception)
            {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }
}
