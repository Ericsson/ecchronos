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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

public class TestDataCenterAndVnodeRepairResourceFactory
{
    @Test
    public void testSingleDataCenterHost()
    {
        UUID nodeId = UUID.fromString("f4678229-61eb-4a06-9db6-49e116c8ece0");
        Node node = mockNode("DC1", nodeId);
        RepairResource repairResourceVnode = new RepairResource("DC1", nodeId.toString());
        RepairResource repairResourceLegacy = new RepairResource("DC1", "DC1");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node);

        RepairResourceFactory repairResourceFactory = RepairLockType.DATACENTER_AND_VNODE.getLockFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceVnode, repairResourceLegacy);
    }

    @Test
    public void testMultipleDataCenterHosts()
    {
        UUID nodeId = UUID.fromString("f4678229-61eb-4a06-9db6-49e116c8ece0");
        UUID nodeId2 = UUID.fromString("1dbe1c4f-81a8-426b-b599-cfcc26fca224");
        Node node = mockNode("DC1", nodeId);
        Node node2 = mockNode("DC2", nodeId2);
        RepairResource repairResourceVnodeDc1 = new RepairResource("DC1", nodeId.toString());
        RepairResource repairResourceVnodeDc2 = new RepairResource("DC2", nodeId2.toString());
        RepairResource repairResourceLegacyDc1 = new RepairResource("DC1", "DC1");
        RepairResource repairResourceLegacyDc2 = new RepairResource("DC2", "DC2");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node, node2);

        RepairResourceFactory repairResourceFactory = RepairLockType.DATACENTER_AND_VNODE.getLockFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceVnodeDc1, repairResourceVnodeDc2,
                repairResourceLegacyDc1, repairResourceLegacyDc2);
    }

    private ReplicaRepairGroup generateReplicaRepairGroup(Node... nodes)
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        return new ReplicaRepairGroup(ImmutableSet.copyOf(nodes), ImmutableList.of(range), System.currentTimeMillis());
    }

    private Node mockNode(String dataCenter, UUID nodeId)
    {
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(nodeId);
        return node;
    }
}
