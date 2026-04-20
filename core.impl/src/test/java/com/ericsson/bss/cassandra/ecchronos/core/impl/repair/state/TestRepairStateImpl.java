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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.PostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroupFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairStateImpl
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";

    private static final TableReference TABLE_REFERENCE = tableReference(KEYSPACE_NAME, TABLE_NAME);

    @Mock
    private Node myNode;

    @Mock
    private VnodeRepairStateFactory myVnodeRepairStateFactory;

    @Mock
    private HostStates myHostStates;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicaRepairGroupFactory myReplicaRepairGroupFactory;

    @Mock
    private PostUpdateHook myPostUpdateHook;

    @Mock
    private DriverNode myDriverNode;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(7, TimeUnit.DAYS)
                .withRepairWarningTime(8, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .build();
    }

    @Test
    public void testPostUpdateHookCalledOnSuccessfulUpdate()
    {
        setupSuccessfulStateFactory();

        new RepairStateImpl(myNode, TABLE_REFERENCE, myRepairConfiguration,
                myVnodeRepairStateFactory, myHostStates, myTableRepairMetrics,
                myReplicaRepairGroupFactory, myPostUpdateHook);

        // verify - constructor calls update() which should call postUpdateHook
        verify(myPostUpdateHook).postUpdate(any(), any());
    }

    @Test
    public void testPostUpdateHookCalledWhenUpdateThrowsException()
    {
        // setup - first call succeeds (constructor), second call throws
        long repairIntervalMs = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - repairIntervalMs - 1;
        VnodeRepairStates vnodeRepairStates = createVnodeRepairStates(lastRepairedAt);

        when(myVnodeRepairStateFactory.calculateNewState(any(), eq(TABLE_REFERENCE), any(), anyLong()))
                .thenReturn(vnodeRepairStates)
                .thenThrow(new RuntimeException("No node was available to execute the query"));
        when(myReplicaRepairGroupFactory.generateReplicaRepairGroups(any()))
                .thenReturn(Collections.emptyList());

        RepairStateImpl repairState = new RepairStateImpl(myNode, TABLE_REFERENCE, myRepairConfiguration,
                myVnodeRepairStateFactory, myHostStates, myTableRepairMetrics,
                myReplicaRepairGroupFactory, myPostUpdateHook);

        // verify - constructor calls update() -> postUpdateHook called once
        verify(myPostUpdateHook, times(1)).postUpdate(any(), any());

        // act - second update() should throw exception
        boolean exceptionThrown = false;
        try
        {
            repairState.update();
        }
        catch (RuntimeException e)
        {
            exceptionThrown = true;
        }

        // verify - postUpdateHook should still be called despite the exception
        verify(myPostUpdateHook, times(2)).postUpdate(any(), any());
        assertThat(exceptionThrown).isTrue();
    }

    private void setupSuccessfulStateFactory()
    {
        long now = System.currentTimeMillis();
        VnodeRepairStates vnodeRepairStates = createVnodeRepairStates(now);

        when(myVnodeRepairStateFactory.calculateNewState(any(), eq(TABLE_REFERENCE), any(), anyLong()))
                .thenReturn(vnodeRepairStates);
        when(myReplicaRepairGroupFactory.generateReplicaRepairGroups(any()))
                .thenReturn(Collections.emptyList());
    }

    private VnodeRepairStates createVnodeRepairStates(final long lastRepairedAt)
    {
        VnodeRepairState vnodeRepairState = new VnodeRepairState(
                new LongTokenRange(1, 2),
                ImmutableSet.of(myDriverNode),
                lastRepairedAt,
                lastRepairedAt);
        return VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState)).build();
    }
}
