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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

public class TestPrimaryRangeRepairResourceFactory
{
    private static DriverNode mockNode(final String dataCenter)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(UUID.randomUUID());
        return node;
    }

    private static TableReference mockTable(final String keyspace, final String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        return tableReference;
    }

    private static ReplicaRepairGroup group(final DriverNode... nodes)
    {
        return new ReplicaRepairGroup(ImmutableSet.copyOf(nodes), ImmutableList.of(), 0L);
    }

    private static Set<String> describe(final Set<RepairResource> resources)
    {
        return resources.stream()
                .map(r -> r.getDataCenter() + "/" + r.getResourceName(1) + "/" + r.getMaxSlots())
                .collect(Collectors.toSet());
    }

    @Test
    public void testTableLockAndOneLockPerDataCenterAllSingleSlot()
    {
        Set<RepairResource> resources = new PrimaryRangeRepairResourceFactory(mockTable("ks", "tbl"))
                .getRepairResources(group(mockNode("DC1"), mockNode("DC1"), mockNode("DC2")));

        assertThat(describe(resources)).containsExactlyInAnyOrder(
                "global/RepairResource-ks.tbl-1/1",
                "DC1/RepairResource-PrimaryRange-DC1-1/1",
                "DC2/RepairResource-PrimaryRange-DC2-1/1");
    }

    @Test
    public void testNoPerNodeLocks()
    {
        DriverNode nodeA = mockNode("DC1");
        DriverNode nodeB = mockNode("DC1");

        Set<RepairResource> resources = new PrimaryRangeRepairResourceFactory(mockTable("ks", "tbl"))
                .getRepairResources(group(nodeA, nodeB));

        // The table lock and the single datacenter lock only, however many replicas the group has.
        assertThat(resources).hasSize(2);
        assertThat(resources).extracting(r -> r.getResourceName(1))
                .noneMatch(name -> name.contains(nodeA.getId().toString()) || name.contains(nodeB.getId().toString()));
    }

    @Test
    public void testSharesTheTableLockWithIncrementalRepair()
    {
        ReplicaRepairGroup group = group(mockNode("DC1"));
        TableReference table = mockTable("ks", "tbl");

        RepairResource primaryRangeTableLock = new PrimaryRangeRepairResourceFactory(table)
                .getRepairResources(group).stream().filter(r -> "global".equals(r.getDataCenter())).findFirst()
                .orElseThrow();
        RepairResource incrementalTableLock = new IncrementalRepairResourceFactory(table)
                .getRepairResources(group).stream().filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();

        // Equal, and the same lock name and slot count, so the two repair types exclude each other for a table.
        assertThat(primaryRangeTableLock).isEqualTo(incrementalTableLock);
        assertThat(primaryRangeTableLock.getResourceName(1)).isEqualTo(incrementalTableLock.getResourceName(1));
        assertThat(primaryRangeTableLock.getMaxSlots()).isEqualTo(incrementalTableLock.getMaxSlots());
    }

    @Test
    public void testDataCenterLockDoesNotCollideWithTheVnodeDataCenterLock()
    {
        ReplicaRepairGroup group = group(mockNode("DC1"), mockNode("DC2"));

        Set<String> primaryRangeLockNames = new PrimaryRangeRepairResourceFactory(mockTable("ks", "tbl"))
                .getRepairResources(group).stream()
                .filter(r -> !"global".equals(r.getDataCenter()))
                .map(r -> r.getResourceName(1)).collect(Collectors.toSet());
        Set<String> vnodeDataCenterLockNames = RepairLockType.DATACENTER.getLockFactory()
                .getRepairResources(group).stream().map(r -> r.getResourceName(1)).collect(Collectors.toSet());

        assertThat(vnodeDataCenterLockNames).containsExactlyInAnyOrder("RepairResource-DC1-1", "RepairResource-DC2-1");
        assertThat(primaryRangeLockNames).isNotEmpty().doesNotContainAnyElementsOf(vnodeDataCenterLockNames);
    }

    @Test
    public void testTableLockIsDataCenterIndependent()
    {
        TableReference table = mockTable("ks", "tbl");

        RepairResource tableLockDc1 = new PrimaryRangeRepairResourceFactory(table)
                .getRepairResources(group(mockNode("DC1"))).stream()
                .filter(r -> "global".equals(r.getDataCenter())).findFirst().orElseThrow();
        RepairResource tableLockDc2 = new PrimaryRangeRepairResourceFactory(table)
                .getRepairResources(group(mockNode("DC2"))).stream()
                .filter(r -> "global".equals(r.getDataCenter())).findFirst().orElseThrow();

        assertThat(tableLockDc1).isEqualTo(tableLockDc2);
    }

    @Test
    public void testDifferentTablesAndKeyspacesGetDifferentTableLocks()
    {
        ReplicaRepairGroup group = group(mockNode("DC1"));

        Set<String> ks1Tbl1 = describe(new PrimaryRangeRepairResourceFactory(mockTable("ks1", "tbl1"))
                .getRepairResources(group));
        Set<String> ks1Tbl2 = describe(new PrimaryRangeRepairResourceFactory(mockTable("ks1", "tbl2"))
                .getRepairResources(group));
        Set<String> ks2Tbl1 = describe(new PrimaryRangeRepairResourceFactory(mockTable("ks2", "tbl1"))
                .getRepairResources(group));

        assertThat(ks1Tbl1).contains("global/RepairResource-ks1.tbl1-1/1");
        assertThat(ks1Tbl2).contains("global/RepairResource-ks1.tbl2-1/1").doesNotContain(
                "global/RepairResource-ks1.tbl1-1/1");
        assertThat(ks2Tbl1).contains("global/RepairResource-ks2.tbl1-1/1").doesNotContain(
                "global/RepairResource-ks1.tbl1-1/1");
    }
}
