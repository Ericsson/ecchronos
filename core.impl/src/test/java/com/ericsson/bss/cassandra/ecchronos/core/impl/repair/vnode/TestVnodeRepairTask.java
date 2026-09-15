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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestVnodeRepairTask
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
    private Node myNode;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myNode.getHostId()).thenReturn(myNodeId);
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairType(RepairType.VNODE)
                .build();
    }

    @Test
    public void testUnknownRangesTerminateOnlyTheSpecificNode() throws Throwable
    {
        // A token range that never receives a finished/failed notification is "unknown" at verifyRepair time.
        Set<LongTokenRange> ranges = ImmutableSet.of(LongTokenRange.of(1, 100));
        Set<DriverNode> replicas = ImmutableSet.of(mock(DriverNode.class));

        VnodeRepairTask task = new VnodeRepairTask(
                myNode, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                RepairHistory.NO_OP, ranges, replicas, myJobId);

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(() -> invokeVerifyRepair(task))
                .withMessageContaining("Unknown status");

        // Blast radius must be bounded to the node that ran the repair (issue #1815).
        verify(myJmxProxy).forceTerminateAllRepairSessionsInSpecificNode(myNodeId);
        verify(myJmxProxy, never()).forceTerminateAllRepairSessions();
    }

    private void invokeVerifyRepair(final VnodeRepairTask task) throws Throwable
    {
        Method m = VnodeRepairTask.class.getDeclaredMethod("verifyRepair", DistributedJmxProxy.class);
        m.setAccessible(true);
        try
        {
            m.invoke(task, myJmxProxy);
        }
        catch (InvocationTargetException e)
        {
            throw e.getCause();
        }
    }
}
