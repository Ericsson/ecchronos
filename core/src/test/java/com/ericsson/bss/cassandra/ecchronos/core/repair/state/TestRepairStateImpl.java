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

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairStateImpl
{
    private static final TableReference tableReference = tableReference("ks", "tb");

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

    @Captor
    private ArgumentCaptor<List<VnodeRepairState>> repairGroupCaptor;

    @Test
    public void testInitialEmptyState()
    {
        long expectedAtLeastRepairedAt = System.currentTimeMillis();
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        Node host = mockNode("DC1");
        when(mockHostStates.isUp(eq(host))).thenReturn(true);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(host), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull())).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(repairGroupCaptor.capture())).thenReturn(Lists.emptyList());

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();

        assertThat(repairGroupCaptor.getValue()).isEmpty();
        assertRepairStateSnapshot(repairStateSnapshot, expectedAtLeastRepairedAt, Lists.emptyList(), vnodeRepairStates);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(0));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(repairStateSnapshot.lastCompletedAt()));
        verify(mockTableRepairMetrics).remainingRepairTime(eq(tableReference), eq(0L));
        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);
    }

    @Test
    public void testPartiallyRepaired()
    {
        long now = System.currentTimeMillis();
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);
        long expectedAtLeastRepairedAt = now - repairIntervalInMs;
        long vnodeRepairTime = 5L;
        long needRepairFinishedAt = expectedAtLeastRepairedAt + vnodeRepairTime;

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        Node node = mockNode("DC1");
        when(mockHostStates.isUp(eq(node))).thenReturn(true);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), expectedAtLeastRepairedAt, needRepairFinishedAt);
        VnodeRepairState repairedVnodeRepairState = new VnodeRepairState(new LongTokenRange(2, 3), ImmutableSet.of(node), now, now);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, repairedVnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull())).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(repairGroupCaptor.capture())).thenReturn(Collections.singletonList(mockReplicaRepairGroup));

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();

        List<VnodeRepairState> capturedVnodeStates = repairGroupCaptor.getValue();

        assertThat(capturedVnodeStates).hasSize(1);
        VnodeRepairState capturedVnodeRepairState = capturedVnodeStates.get(0);

        assertVnodeRepairStateRepairedBefore(vnodeRepairState, capturedVnodeRepairState, System.currentTimeMillis() - repairIntervalInMs);
        assertRepairStateSnapshot(repairStateSnapshot, expectedAtLeastRepairedAt, Collections.singletonList(mockReplicaRepairGroup), vnodeRepairStates);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(1));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(repairStateSnapshot.lastCompletedAt()));
        verify(mockTableRepairMetrics).remainingRepairTime(eq(tableReference), eq(vnodeRepairTime));
        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);
    }

    @Test
    public void testUpdateRepaired()
    {
        long expectedRepairedAt = System.currentTimeMillis();
        long finishedAt = expectedRepairedAt + TimeUnit.SECONDS.toMillis(5);
        long repairIntervalInMs = TimeUnit.HOURS.toMillis(1);

        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(mockNode("DC1")), expectedRepairedAt, finishedAt);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();

        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull())).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(repairGroupCaptor.capture())).thenReturn(Lists.emptyList());

        RepairState repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);

        RepairStateSnapshot repairStateSnapshot = repairState.getSnapshot();

        assertThat(repairGroupCaptor.getValue()).isEmpty();
        assertRepairStateSnapshot(repairStateSnapshot, expectedRepairedAt, Lists.emptyList(), vnodeRepairStates);

        verify(mockPostUpdateHook, times(1)).postUpdate(repairStateSnapshot);

        verify(mockTableRepairMetrics).repairState(eq(tableReference), eq(1), eq(0));
        verify(mockTableRepairMetrics).lastRepairedAt(eq(tableReference), eq(expectedRepairedAt));
        verify(mockTableRepairMetrics).remainingRepairTime(eq(tableReference), eq(0L));
        reset(mockTableRepairMetrics);

        // Perform update
        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), eq(repairStateSnapshot))).thenReturn(vnodeRepairStates);
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
        assertThat(repairStateSnapshot.lastCompletedAt()).isGreaterThanOrEqualTo(expectedAtLeastRepairedAt);
        assertThat(repairStateSnapshot.lastCompletedAt()).isLessThanOrEqualTo(expectedAtMostRepairedAt);
        assertThat(repairStateSnapshot.getRepairGroups()).isEqualTo(replicaRepairGroups);
        assertThat(repairStateSnapshot.canRepair()).isEqualTo(canRepair);
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(vnodeRepairStatesBase.combineWithRepairedAt(repairStateSnapshot.lastCompletedAt()));
    }

    private void assertVnodeRepairStateRepairedBefore(VnodeRepairState baseVnodeRepairState, VnodeRepairState actualVnodeRepairState, long repairedBefore)
    {
        assertThat(actualVnodeRepairState.getTokenRange()).isEqualTo(baseVnodeRepairState.getTokenRange());
        assertThat(actualVnodeRepairState.getReplicas()).isEqualTo(baseVnodeRepairState.getReplicas());

        assertThat(actualVnodeRepairState.lastRepairedAt()).isNotEqualTo(VnodeRepairState.UNREPAIRED);
        assertThat(actualVnodeRepairState.lastRepairedAt()).isLessThanOrEqualTo(repairedBefore);
    }

    @Test
    public void testIsRepairNeeded()
    {
        long startedAt = 9000L;
        long finishedAt = 9500L;
        long repairIntervalInMs = TimeUnit.MILLISECONDS.toMillis(1000);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(mockNode("DC1")), startedAt, finishedAt);
        RepairConfiguration repairConfiguration = repairConfiguration(repairIntervalInMs);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();
        when(mockVnodeRepairStateFactory.calculateNewState(eq(tableReference), isNull())).thenReturn(vnodeRepairStates);
        when(mockReplicaRepairGroupFactory.generateReplicaRepairGroups(repairGroupCaptor.capture())).thenReturn(Lists.emptyList());
        RepairStateImpl repairState = new RepairStateImpl(tableReference, repairConfiguration,
                mockVnodeRepairStateFactory, mockHostStates,
                mockTableRepairMetrics, mockReplicaRepairGroupFactory, mockPostUpdateHook);
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 0L, 9000L)).isFalse();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 0L, 9500L)).isFalse();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 0L, 10000L)).isTrue();

        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 500L, 9000L)).isFalse();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 500L, 9499L)).isFalse();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 500L, 9500L)).isTrue();

        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 1000L, 9000L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 1000L, 9499L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 1000L, 9500L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 1000L, 10000L)).isTrue();
        //Repair takes longer time than interval
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 5000L, 9000L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 5000L, 9499L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 5000L, 9500L)).isTrue();
        assertThat(repairState.isRepairNeeded(vnodeRepairState.lastRepairedAt(), 5000L, 10000L)).isTrue();
    }

    private RepairConfiguration repairConfiguration(long repairIntervalInMs)
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(repairIntervalInMs, TimeUnit.MILLISECONDS)
                .build();
    }

    private Node mockNode(String dataCenter)
    {
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
