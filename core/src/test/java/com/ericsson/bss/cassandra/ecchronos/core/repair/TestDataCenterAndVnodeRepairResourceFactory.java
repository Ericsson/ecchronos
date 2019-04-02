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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

public class TestDataCenterAndVnodeRepairResourceFactory
{
    @Test
    public void testSingleDataCenterHost()
    {
        UUID hostId = UUID.fromString("f4678229-61eb-4a06-9db6-49e116c8ece0");
        Host host = mockHost("DC1", hostId);
        RepairResource repairResourceVnode = new RepairResource("DC1", hostId.toString());
        RepairResource repairResourceLegacy = new RepairResource("DC1", "DC1");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(host);

        RepairResourceFactory repairResourceFactory = RepairLockType.DATACENTER_AND_VNODE.getLockFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceVnode, repairResourceLegacy);
    }

    @Test
    public void testMultipleDataCenterHosts()
    {
        UUID hostId = UUID.fromString("f4678229-61eb-4a06-9db6-49e116c8ece0");
        UUID hostId2 = UUID.fromString("1dbe1c4f-81a8-426b-b599-cfcc26fca224");
        Host host = mockHost("DC1", hostId);
        Host host2 = mockHost("DC2", hostId2);
        RepairResource repairResourceVnodeDc1 = new RepairResource("DC1", hostId.toString());
        RepairResource repairResourceVnodeDc2 = new RepairResource("DC2", hostId2.toString());
        RepairResource repairResourceLegacyDc1 = new RepairResource("DC1", "DC1");
        RepairResource repairResourceLegacyDc2 = new RepairResource("DC2", "DC2");
        ReplicaRepairGroup replicaRepairGroup = generateReplicaRepairGroup(host, host2);

        RepairResourceFactory repairResourceFactory = RepairLockType.DATACENTER_AND_VNODE.getLockFactory();

        assertThat(repairResourceFactory.getRepairResources(replicaRepairGroup)).containsExactlyInAnyOrder(repairResourceVnodeDc1, repairResourceVnodeDc2,
                repairResourceLegacyDc1, repairResourceLegacyDc2);
    }

    private ReplicaRepairGroup generateReplicaRepairGroup(Host... hosts)
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        return new ReplicaRepairGroup(ImmutableSet.copyOf(hosts), ImmutableList.of(range));
    }

    private Host mockHost(String dataCenter, UUID hostId)
    {
        Host host = mock(Host.class);
        doReturn(dataCenter).when(host).getDatacenter();
        doReturn(hostId).when(host).getHostId();
        return host;
    }
}
