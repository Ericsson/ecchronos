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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

/**
 * Pins the {@link RepairResource} sets produced for each {@link RepairLockType}. These are the lock sets used by
 * vnode repair (the lock type is configurable). They must not change as a side effect of refactoring how repair
 * types are dispatched; see issue #1989.
 */
public class TestRepairLockTypeResources
{
    private UUID myNodeIdA;
    private UUID myNodeIdB;
    private UUID myNodeIdC;
    private ReplicaRepairGroup myGroup;

    @Before
    public void setup()
    {
        myNodeIdA = UUID.randomUUID();
        myNodeIdB = UUID.randomUUID();
        myNodeIdC = UUID.randomUUID();
        // Two replicas in DC1 and one in DC2, so the datacenter and node dimensions can not be confused.
        myGroup = new ReplicaRepairGroup(
                ImmutableSet.of(mockNode("DC1", myNodeIdA), mockNode("DC1", myNodeIdB), mockNode("DC2", myNodeIdC)),
                ImmutableList.of(), 0L);
    }

    private static DriverNode mockNode(final String dataCenter, final UUID id)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(id);
        return node;
    }

    private Set<RepairResource> expectedDataCenterResources()
    {
        return Set.of(new RepairResource("DC1", "DC1"), new RepairResource("DC2", "DC2"));
    }

    private Set<RepairResource> expectedNodeResources()
    {
        return Set.of(
                new RepairResource("DC1", myNodeIdA.toString()),
                new RepairResource("DC1", myNodeIdB.toString()),
                new RepairResource("DC2", myNodeIdC.toString()));
    }

    @Test
    public void testDataCenterLockTypeLocksOneResourcePerDataCenter()
    {
        Set<RepairResource> resources = RepairLockType.DATACENTER.getLockFactory().getRepairResources(myGroup);

        assertThat(resources).containsExactlyInAnyOrderElementsOf(expectedDataCenterResources());
    }

    @Test
    public void testDataCenterLockTypeResourceNames()
    {
        Set<RepairResource> resources = RepairLockType.DATACENTER.getLockFactory().getRepairResources(myGroup);

        assertThat(resources).extracting(r -> r.getResourceName(1))
                .containsExactlyInAnyOrder("RepairResource-DC1-1", "RepairResource-DC2-1");
    }

    @Test
    public void testVnodeLockTypeLocksOneResourcePerReplica()
    {
        Set<RepairResource> resources = RepairLockType.VNODE.getLockFactory().getRepairResources(myGroup);

        assertThat(resources).containsExactlyInAnyOrderElementsOf(expectedNodeResources());
    }

    @Test
    public void testVnodeLockTypeResourceNames()
    {
        Set<RepairResource> resources = RepairLockType.VNODE.getLockFactory().getRepairResources(myGroup);

        assertThat(resources).extracting(r -> r.getResourceName(1))
                .containsExactlyInAnyOrder(
                        "RepairResource-" + myNodeIdA + "-1",
                        "RepairResource-" + myNodeIdB + "-1",
                        "RepairResource-" + myNodeIdC + "-1");
    }

    @Test
    public void testDataCenterAndVnodeLockTypeIsTheUnionOfBoth()
    {
        Set<RepairResource> resources =
                RepairLockType.DATACENTER_AND_VNODE.getLockFactory().getRepairResources(myGroup);

        Set<RepairResource> expected = new HashSet<>();
        expected.addAll(expectedDataCenterResources());
        expected.addAll(expectedNodeResources());
        assertThat(resources).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testAllNonIncrementalLockTypesUseTheGlobalSlotCount()
    {
        for (RepairLockType lockType : RepairLockType.values())
        {
            Set<RepairResource> resources = lockType.getLockFactory().getRepairResources(myGroup);

            assertThat(resources)
                    .as("lock type %s", lockType)
                    .isNotEmpty()
                    .allSatisfy(r -> assertThat(r.getMaxSlots()).isEqualTo(RepairResource.USE_GLOBAL_SLOTS));
        }
    }

    @Test
    public void testSingleDataCenterGroupProducesOneDataCenterResource()
    {
        ReplicaRepairGroup singleDcGroup = new ReplicaRepairGroup(
                ImmutableSet.of(mockNode("DC1", myNodeIdA), mockNode("DC1", myNodeIdB)), ImmutableList.of(), 0L);

        assertThat(RepairLockType.DATACENTER.getLockFactory().getRepairResources(singleDcGroup))
                .containsExactly(new RepairResource("DC1", "DC1"));
        assertThat(RepairLockType.VNODE.getLockFactory().getRepairResources(singleDcGroup)).hasSize(2);
        assertThat(RepairLockType.DATACENTER_AND_VNODE.getLockFactory().getRepairResources(singleDcGroup))
                .hasSize(3);
    }

    @Test
    public void testLockTypesCoverExactlyTheKnownSet()
    {
        // If a lock type is added, the pinning tests above must be extended to cover it.
        assertThat(RepairLockType.values())
                .containsExactlyInAnyOrder(
                        RepairLockType.DATACENTER, RepairLockType.VNODE, RepairLockType.DATACENTER_AND_VNODE);
    }
}
