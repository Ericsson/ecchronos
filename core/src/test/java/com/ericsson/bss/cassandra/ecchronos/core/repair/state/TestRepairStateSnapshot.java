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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairStateSnapshot
{
    @Mock
    private ReplicaRepairGroup mockRepairGroup;

    @Mock
    private VnodeRepairStates mockVnodeRepairStates;

    @Test
    public void testCanRepairFalse()
    {
        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroup(null)
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastRepairedAt(VnodeRepairState.UNREPAIRED)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isFalse();
        assertThat(repairStateSnapshot.getRepairGroup()).isNull();
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }

    @Test
    public void testCanRepairTrue()
    {
        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroup(mockRepairGroup)
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastRepairedAt(VnodeRepairState.UNREPAIRED)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isTrue();
        assertThat(repairStateSnapshot.getRepairGroup()).isEqualTo(mockRepairGroup);
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }

    @Test
    public void testDifferentRepairedAt()
    {
        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroup(null)
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastRepairedAt(1234L)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isFalse();
        assertThat(repairStateSnapshot.getRepairGroup()).isNull();
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastRepairedAt()).isEqualTo(1234L);
    }
}
