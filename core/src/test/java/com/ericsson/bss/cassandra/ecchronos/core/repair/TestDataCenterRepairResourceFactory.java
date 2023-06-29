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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestDataCenterRepairResourceFactory
{
    @Test
    public void testSingleDataCenter()
    {
        Host host = mockHost("DC1");
        RepairResource repairResourceDc1 = new RepairResource("DC1", "DC1");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(host);

        RepairResourceFactory repairResourceFactory = new DataCenterRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactly(repairResourceDc1);
    }

    @Test
    public void testMultipleDataCenters()
    {
        Host host = mockHost("DC1");
        Host host2 = mockHost("DC2");
        RepairResource repairResourceDc1 = new RepairResource("DC1", "DC1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "DC2");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(host, host2);

        RepairResourceFactory repairResourceFactory = new DataCenterRepairResourceFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceDc1, repairResourceDc2);
    }

    private ReplicaRepairGroup generateReplicaRepairGroup(Host... hosts)
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        return new ReplicaRepairGroup(ImmutableSet.copyOf(hosts), ImmutableList.of(range), System.currentTimeMillis());
    }

    private Host mockHost(String dataCenter)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        return host;
    }
}
