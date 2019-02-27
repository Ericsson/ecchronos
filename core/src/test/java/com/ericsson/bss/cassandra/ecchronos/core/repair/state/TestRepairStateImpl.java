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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairStateImpl
{
    private static final TableReference tableReference = new TableReference("ks", "tb");

    @Mock
    private VnodeRepairStateFactory mockVnodeRepairStateFactory;

    @Mock
    private HostStates mockHostStates;

    @Mock
    private TableRepairMetrics mockTableRepairMetrics;

    @Mock
    private ReplicaRepairGroupFactory mockReplicaRepairGroupFactory;

    @Mock
    private ReplicaRepairGroup mockReplicaRepairGroup;

    @Mock
    private PostUpdateHook mockPostUpdateHook;

    @Before
    public void init()
    {
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(any())).thenReturn(Collections.singletonList(mockReplicaRepairGroup));
    }

    @Test
    public void testInitialEmptyState()
    {
        long expectedAtLeastRepairedAt = System.currentTimeMillis();
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        Host host = mockHost("DC1");
        when(mockHostStates.isUp(eq(host))).thenReturn(true);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), Sets.newHashSet(host), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull(RepairStateSnapshot.class))).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(any())).thenReturn(Lists.emptyList());

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();

        assertRepairStateSnapshot(repairStateSnapshot, expectedAtLeastRepairedAt, Lists.emptyList(), vnodeRepairStates);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(0));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(repairStateSnapshot.lastRepairedAt()));
        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);
    }

    @Test
    public void testPartiallyRepaired()
    {
        long now = System.currentTimeMillis();
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);
        long expectedAtLeastRepairedAt = now - repairIntervalInMs;

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        Host host = mockHost("DC1");
        when(mockHostStates.isUp(eq(host))).thenReturn(true);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), Sets.newHashSet(host), VnodeRepairState.UNREPAIRED);
        VnodeRepairState repairedVnodeRepairState = new VnodeRepairState(new LongTokenRange(2, 3), Sets.newHashSet(host), now);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(Arrays.asList(vnodeRepairState, repairedVnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull(RepairStateSnapshot.class))).thenReturn(vnodeRepairStates);

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();

        assertRepairStateSnapshot(repairStateSnapshot, expectedAtLeastRepairedAt, Collections.singletonList(mockReplicaRepairGroup), vnodeRepairStates);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(1));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(repairStateSnapshot.lastRepairedAt()));
        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);
    }

    @Test
    public void testUpdateRepaired()
    {
        long expectedRepairedAt = System.currentTimeMillis();
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), Sets.newHashSet(mockHost("DC1")), expectedRepairedAt);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull(RepairStateSnapshot.class))).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(any())).thenReturn(Lists.emptyList());

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();
        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);
        assertRepairStateSnapshot(repairStateSnapshot, expectedRepairedAt, Lists.emptyList(), vnodeRepairStates);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(0));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(repairStateSnapshot.lastRepairedAt()));
        reset(mockTableRepairMetrics);

        // Perform update
        repairState.update();

        RepairStateSnapshot updatedRepairStateSnapshot = repairState.getSnapshot();
        assertThat(updatedRepairStateSnapshot).isSameAs(repairStateSnapshot);
        verifyNoMoreInteractions(mockTableRepairMetrics);
        verify(mockPostUpdateHook, times(2)).postUpdate(updatedRepairStateSnapshot);
    }

    private void assertRepairStateSnapshot(RepairStateSnapshot repairStateSnapshot, long expectedAtLeastRepairedAt, List<ReplicaRepairGroup> replicaRepairGroups, VnodeRepairStates vnodeRepairStatesBase)
    {
        long expectedAtMostRepairedAt = expectedAtLeastRepairedAt + TimeUnit.MINUTES.toMillis(1);
        boolean canRepair = !replicaRepairGroups.isEmpty();

        assertThat(repairStateSnapshot).isNotNull();
        assertThat(repairStateSnapshot.lastRepairedAt()).isGreaterThanOrEqualTo(expectedAtLeastRepairedAt);
        assertThat(repairStateSnapshot.lastRepairedAt()).isLessThanOrEqualTo(expectedAtMostRepairedAt);
        assertThat(repairStateSnapshot.getRepairGroups()).isEqualTo(replicaRepairGroups);
        assertThat(repairStateSnapshot.canRepair()).isEqualTo(canRepair);
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(vnodeRepairStatesBase.combineWithRepairedAt(repairStateSnapshot.lastRepairedAt()));
    }

    private RepairConfiguration repairConfiguration(long repairIntervalInMs)
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(repairIntervalInMs, TimeUnit.MILLISECONDS)
                .build();
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        when(host.getDatacenter()).thenReturn(dataCenter);
        return host;
    }
}
