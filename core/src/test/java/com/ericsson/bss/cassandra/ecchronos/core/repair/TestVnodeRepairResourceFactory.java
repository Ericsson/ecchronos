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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestVnodeRepairResourceFactory
{
    @Test
    public void testSingleDataCenterHost()
    {
        UUID nodeId = UUID.fromString("f4678229-61eb-4a06-9db6-49e116c8ece0");
        Node node = mockNode("DC1", nodeId);
        RepairResource repairResourceVnode = new RepairResource("DC1", nodeId.toString());
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node);

        RepairResourceFactory repairResourceFactory = new VnodeRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactly(repairResourceVnode);
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
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node, node2);

        RepairResourceFactory repairResourceFactory = new VnodeRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceVnodeDc1, repairResourceVnodeDc2);
    }

    private ReplicaRepairGroup generateReplicaRepairGroup(Node... nodes)
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        return new ReplicaRepairGroup(ImmutableSet.copyOf(nodes), ImmutableList.of(range));
    }

    private Node mockNode(String dataCenter, UUID id)
    {
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        when(node.getId()).thenReturn(id);
        return node;
    }
}
