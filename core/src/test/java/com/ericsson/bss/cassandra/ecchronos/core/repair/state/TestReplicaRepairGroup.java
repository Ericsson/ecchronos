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
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestReplicaRepairGroup
{
    @Test
    public void testMultipleDataCenters()
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC2");
        Host host3 = mockHost("DC3");
        Host host4 = mockHost("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1, host2, host3, host4), Collections.singletonList(range));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1", "DC2", "DC3");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(host1, host2, host3, host4);
        assertThat(replicaRepairGroup.iterator()).containsExactly(range);
        assertThat(replicaRepairGroup.getVnodes()).containsExactly(range);
    }

    @Test
    public void testMultipleRanges()
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC1");
        Host host3 = mockHost("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);
        LongTokenRange range3 = new LongTokenRange(5, 6);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(Sets.newHashSet(host1, host2, host3), Arrays.asList(range, range2, range3));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(host1, host2, host3);
        assertThat(replicaRepairGroup.iterator()).containsExactly(range, range2, range3);
        assertThat(replicaRepairGroup.getVnodes()).containsExactly(range, range2, range3);
    }

    @Test
    public void testImmutability()
    {
        Host host1 = mockHost("DC1");
        Host host2 = mockHost("DC1");
        Host host3 = mockHost("DC1");
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(3, 4);
        LongTokenRange range3 = new LongTokenRange(5, 6);

        Set<Host> inputReplicas = new HashSet<>();
        inputReplicas.add(host1);
        inputReplicas.add(host2);

        List<LongTokenRange> inputVnodes = new ArrayList<>();
        inputVnodes.add(range);
        inputVnodes.add(range2);

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(inputReplicas, inputVnodes);

        inputReplicas.add(host3);
        inputVnodes.add(range3);

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(host1, host2);
        assertThat(replicaRepairGroup.iterator()).containsExactly(range, range2);
        assertThat(replicaRepairGroup.getVnodes()).containsExactly(range, range2);
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        return host;
    }
}
