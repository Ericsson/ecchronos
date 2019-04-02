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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

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

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(host1, host2, host3, host4), ImmutableList.of(range));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1", "DC2", "DC3");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(host1, host2, host3, host4);
        assertThat(replicaRepairGroup.iterator()).containsExactly(range);
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

        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(ImmutableSet.of(host1, host2, host3), ImmutableList.of(range, range2, range3));

        assertThat(replicaRepairGroup.getDataCenters()).containsExactlyInAnyOrder("DC1");
        assertThat(replicaRepairGroup.getReplicas()).containsExactlyInAnyOrder(host1, host2, host3);
        assertThat(replicaRepairGroup.iterator()).containsExactly(range, range2, range3);
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        return host;
    }
}
