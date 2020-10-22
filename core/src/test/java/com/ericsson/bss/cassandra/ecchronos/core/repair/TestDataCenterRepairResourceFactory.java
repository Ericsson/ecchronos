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

import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestDataCenterRepairResourceFactory
{
    @Test
    public void testSingleDataCenter()
    {
        Node node = mockNode("DC1");
        RepairResource repairResourceDc1 = new RepairResource("DC1", "DC1");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node);

        RepairResourceFactory repairResourceFactory = new DataCenterRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactly(repairResourceDc1);
    }

    @Test
    public void testMultipleDataCenters()
    {
        Node node = mockNode("DC1");
        Node node2 = mockNode("DC2");
        RepairResource repairResourceDc1 = new RepairResource("DC1", "DC1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "DC2");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(node, node2);

        RepairResourceFactory repairResourceFactory = new DataCenterRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceDc1, repairResourceDc2);
    }

    private ReplicaRepairGroup generateReplicaRepairGroup(Node... nodes)
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        return new ReplicaRepairGroup(ImmutableSet.copyOf(nodes), ImmutableList.of(range));
    }

    private Node mockNode(String dataCenter)
    {
        Node node = mock(Node.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
