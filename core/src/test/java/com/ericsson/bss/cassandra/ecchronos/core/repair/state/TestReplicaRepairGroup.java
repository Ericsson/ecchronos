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

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class TestReplicaRepairGroup
{
    @Test
    public void testMultipleDataCenters()
    {
        DriverNode node1 = mockNode("DC1");
        DriverNode node2 = mockNode("DC2");
        DriverNode node3 = mockNode("DC3");
        DriverNode node4 = mockNode("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(node1, node2, node3, node4), ImmutableList.of(range));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1", "DC2", "DC3");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(node1, node2, node3, node4);
        assertThat(replicaRepairGroup.iterator()).toIterable().containsExactly(range);
    }

    @Test
    public void testMultipleRanges()
    {
        DriverNode node1 = mockNode("DC1");
        DriverNode node2 = mockNode("DC1");
        DriverNode node3 = mockNode("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);
        LongTokenRange range3 = new LongTokenRange(5, 6);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(node1, node2, node3), ImmutableList.of(range, range2, range3));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(node1, node2, node3);
        assertThat(replicaRepairGroup.iterator()).toIterable().containsExactly(range, range2, range3);
    }

    private DriverNode mockNode(String dataCenter)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getDatacenter()).thenReturn(dataCenter);
        return node;
    }
}
