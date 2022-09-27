/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.utils;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairStatsProviderImpl
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";
    private static final TableReference TABLE_REFERENCE = tableReference(KEYSPACE_NAME, TABLE_NAME);

    @Mock
    private VnodeRepairStateFactory vnodeRepairStateFactoryMock;

    @Test
    public void TestGetRepairStatsAllUnrepairedLocal()
    {
        long since = 1234L;
        long to = 1237L;
        DriverNode node = mockNode("DC1");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), VnodeRepairState.UNREPAIRED);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();
        when(vnodeRepairStateFactoryMock.calculateState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, true);
        assertThat(repairStats.repairedRatio).isEqualTo(0);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(0);
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    @Test
    public void TestGetRepairStatsAllUnrepairedClusterWide()
    {
        long since = 1234L;
        long to = 1237L;
        DriverNode node = mockNode("DC1");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), VnodeRepairState.UNREPAIRED);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnodeRepairState))
                .build();
        when(vnodeRepairStateFactoryMock.calculateClusterWideState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, false);
        assertThat(repairStats.repairedRatio).isEqualTo(0);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(0);
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    @Test
    public void TestGetRepairStatsAllRepairedLocal()
    {
        long since = 1234L;
        long to = 1237L;
        long range1StartedAt = 1235L;
        long range1FinishedAt = 1236L;
        long range2StartedAt = 1236L;
        long range2FinishedAt = 1237L;
        DriverNode node = mockNode("DC1");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), range1StartedAt, range1FinishedAt);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(new LongTokenRange(2, 3), ImmutableSet.of(node), range2StartedAt, range2FinishedAt);
        List<VnodeRepairState> repairStates = new ArrayList<>();
        repairStates.add(vnodeRepairState);
        repairStates.add(vnodeRepairState2);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(repairStates)
                .build();
        when(vnodeRepairStateFactoryMock.calculateState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, true);
        assertThat(repairStats.repairedRatio).isEqualTo(1);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(vnodeRepairState.getRepairTime() + vnodeRepairState2.getRepairTime());
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    @Test
    public void TestGetRepairStatsAllRepairedClusterWide()
    {
        long since = 1234L;
        long to = 1237L;
        long range1StartedAt = 1235L;
        long range1FinishedAt = 1236L;
        long range2StartedAt = 1236L;
        long range2FinishedAt = 1237L;
        DriverNode node = mockNode("DC1");
        DriverNode remoteNode = mockNode("DC2");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), range1StartedAt, range1FinishedAt);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(new LongTokenRange(2, 3), ImmutableSet.of(remoteNode), range2StartedAt, range2FinishedAt);
        List<VnodeRepairState> repairStates = new ArrayList<>();
        repairStates.add(vnodeRepairState);
        repairStates.add(vnodeRepairState2);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(repairStates)
                .build();
        when(vnodeRepairStateFactoryMock.calculateClusterWideState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, false);
        assertThat(repairStats.repairedRatio).isEqualTo(1);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(vnodeRepairState.getRepairTime() + vnodeRepairState2.getRepairTime());
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    @Test
    public void TestGetRepairStatsSomeRepairedSomeUnrepairedLocal()
    {
        long range1StartedAt = 1234L;
        long range1FinishedAt = 1235L;
        long since = 1235L;
        long to = 1237L;
        long range2StartedAt = 1235L;
        long range2FinishedAt = 1236L;
        DriverNode node = mockNode("DC1");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), range1StartedAt, range1FinishedAt);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(new LongTokenRange(2, 3), ImmutableSet.of(node), range2StartedAt, range2FinishedAt);
        List<VnodeRepairState> repairStates = new ArrayList<>();
        repairStates.add(vnodeRepairState);
        repairStates.add(vnodeRepairState2);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(repairStates)
                .build();
        when(vnodeRepairStateFactoryMock.calculateState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, true);
        assertThat(repairStats.repairedRatio).isEqualTo(0.5);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(vnodeRepairState2.getRepairTime());
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    @Test
    public void TestGetRepairStatsSomeRepairedSomeUnrepairedClusterWide()
    {
        long range1StartedAt = 1234L;
        long range1FinishedAt = 1235L;
        long since = 1235L;
        long to = 1237L;
        long range2StartedAt = 1235L;
        long range2FinishedAt = 1236L;
        DriverNode node = mockNode("DC1");
        DriverNode remoteNode = mockNode("DC2");
        VnodeRepairState vnodeRepairState = new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node), range1StartedAt, range1FinishedAt);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(new LongTokenRange(2, 3), ImmutableSet.of(remoteNode), range2StartedAt, range2FinishedAt);
        List<VnodeRepairState> repairStates = new ArrayList<>();
        repairStates.add(vnodeRepairState);
        repairStates.add(vnodeRepairState2);
        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(repairStates)
                .build();
        when(vnodeRepairStateFactoryMock.calculateClusterWideState(eq(TABLE_REFERENCE), eq(to), eq(since))).thenReturn(vnodeRepairStates);
        RepairStatsProvider repairStatsProvider = new RepairStatsProviderImpl(vnodeRepairStateFactoryMock);
        RepairStats repairStats = repairStatsProvider.getRepairStats(TABLE_REFERENCE, since, to, false);
        assertThat(repairStats.repairedRatio).isEqualTo(0.5);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(vnodeRepairState2.getRepairTime());
        assertThat(repairStats.keyspace).isEqualTo(KEYSPACE_NAME);
        assertThat(repairStats.table).isEqualTo(TABLE_NAME);
    }

    private DriverNode mockNode(String dataCenter)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
