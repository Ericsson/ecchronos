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

public class TestIncrementalRepairResourceFactory
{
    private static DriverNode mockNode(final String dataCenter, final UUID id)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(id);
        return node;
    }

    private static TableReference mockTable(final String keyspace, final String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        return tableReference;
    }

    @Test
    public void testTableResourceHasSingleSlotAndPerNodeResourcesUseGlobal()
    {
        UUID nodeIdA = UUID.randomUUID();
        UUID nodeIdB = UUID.randomUUID();
        DriverNode nodeA = mockNode("DC1", nodeIdA);
        DriverNode nodeB = mockNode("DC1", nodeIdB);

        ReplicaRepairGroup group = new ReplicaRepairGroup(
                ImmutableSet.of(nodeA, nodeB), ImmutableList.of(), 0L);

        IncrementalRepairResourceFactory factory =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl"));

        Set<RepairResource> resources = factory.getRepairResources(group);

        // One table resource (single-DC group) + two per-node resources
        assertThat(resources).hasSize(3);

        Set<RepairResource> tableResources = resources.stream()
                .filter(r -> r.getMaxSlots() == 1)
                .collect(Collectors.toSet());
        assertThat(tableResources).hasSize(1);
        assertThat(tableResources.iterator().next().getResourceName(1))
                .isEqualTo("RepairResource-ks.tbl-1");

        Set<RepairResource> nodeResources = resources.stream()
                .filter(r -> r.getMaxSlots() == RepairResource.USE_GLOBAL_SLOTS)
                .collect(Collectors.toSet());
        assertThat(nodeResources).hasSize(2);
        assertThat(nodeResources.stream().map(r -> r.getResourceName(1)))
                .containsExactlyInAnyOrder(
                        "RepairResource-" + nodeIdA + "-1",
                        "RepairResource-" + nodeIdB + "-1");
    }

    @Test
    public void testDifferentTablesProduceDifferentTableResources()
    {
        DriverNode node = mockNode("DC1", UUID.randomUUID());
        ReplicaRepairGroup group = new ReplicaRepairGroup(
                ImmutableSet.of(node), ImmutableList.of(), 0L);

        Set<RepairResource> resourcesTbl1 =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl1")).getRepairResources(group);
        Set<RepairResource> resourcesTbl2 =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl2")).getRepairResources(group);

        RepairResource table1 = resourcesTbl1.stream().filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();
        RepairResource table2 = resourcesTbl2.stream().filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();

        // Different tables -> different table resources -> no collision
        assertThat(table1).isNotEqualTo(table2);
    }

    @Test
    public void testSameTableProducesEqualTableResource()
    {
        DriverNode nodeA = mockNode("DC1", UUID.randomUUID());
        DriverNode nodeB = mockNode("DC1", UUID.randomUUID());
        ReplicaRepairGroup groupA = new ReplicaRepairGroup(ImmutableSet.of(nodeA), ImmutableList.of(), 0L);
        ReplicaRepairGroup groupB = new ReplicaRepairGroup(ImmutableSet.of(nodeB), ImmutableList.of(), 0L);

        Set<RepairResource> resourcesA =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl")).getRepairResources(groupA);
        Set<RepairResource> resourcesB =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl")).getRepairResources(groupB);

        RepairResource tableA = resourcesA.stream().filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();
        RepairResource tableB = resourcesB.stream().filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();

        // Same table across different replica nodes -> same table resource -> collide -> serialized
        assertThat(tableA).isEqualTo(tableB);
    }

    @Test
    public void testMultiDataCenterProducesSingleGlobalTableResource()
    {
        DriverNode nodeDc1 = mockNode("DC1", UUID.randomUUID());
        DriverNode nodeDc2 = mockNode("DC2", UUID.randomUUID());
        ReplicaRepairGroup group = new ReplicaRepairGroup(
                ImmutableSet.of(nodeDc1, nodeDc2), ImmutableList.of(), 0L);

        Set<RepairResource> resources =
                new IncrementalRepairResourceFactory(mockTable("ks", "tbl")).getRepairResources(group);

        Set<RepairResource> tableResources = resources.stream()
                .filter(r -> r.getMaxSlots() == 1)
                .collect(Collectors.toSet());
        // A single, datacenter-independent table resource regardless of how many DCs the replicas span
        assertThat(tableResources).hasSize(1);
        assertThat(tableResources.iterator().next().getResourceName(1))
                .isEqualTo("RepairResource-ks.tbl-1");
    }

    @Test
    public void testTableResourceIsDataCenterIndependent()
    {
        // Same table replicated in different datacenters must still yield the SAME table lock resource,
        // so the correctness lock serializes across datacenters.
        DriverNode nodeDc1 = mockNode("DC1", UUID.randomUUID());
        DriverNode nodeDc2 = mockNode("DC2", UUID.randomUUID());
        ReplicaRepairGroup groupDc1 = new ReplicaRepairGroup(ImmutableSet.of(nodeDc1), ImmutableList.of(), 0L);
        ReplicaRepairGroup groupDc2 = new ReplicaRepairGroup(ImmutableSet.of(nodeDc2), ImmutableList.of(), 0L);

        RepairResource tableDc1 = new IncrementalRepairResourceFactory(mockTable("ks", "tbl"))
                .getRepairResources(groupDc1).stream()
                .filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();
        RepairResource tableDc2 = new IncrementalRepairResourceFactory(mockTable("ks", "tbl"))
                .getRepairResources(groupDc2).stream()
                .filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();

        assertThat(tableDc1).isEqualTo(tableDc2);
    }

    @Test
    public void testSameTableNameDifferentKeyspacesDoNotCollide()
    {
        DriverNode node = mockNode("DC1", UUID.randomUUID());
        ReplicaRepairGroup group = new ReplicaRepairGroup(ImmutableSet.of(node), ImmutableList.of(), 0L);

        RepairResource ks1Table = new IncrementalRepairResourceFactory(mockTable("ks1", "tbl"))
                .getRepairResources(group).stream()
                .filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();
        RepairResource ks2Table = new IncrementalRepairResourceFactory(mockTable("ks2", "tbl"))
                .getRepairResources(group).stream()
                .filter(r -> r.getMaxSlots() == 1).findFirst().orElseThrow();

        // Same table name in different keyspaces -> distinct table resources (keyspace.table naming)
        assertThat(ks1Table).isNotEqualTo(ks2Table);
        assertThat(ks1Table.getResourceName(1)).isEqualTo("RepairResource-ks1.tbl-1");
        assertThat(ks2Table.getResourceName(1)).isEqualTo("RepairResource-ks2.tbl-1");
    }
}
