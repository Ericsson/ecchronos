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

import java.util.Collections;

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
                .withReplicaRepairGroups(Collections.emptyList())
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastCompletedAt(VnodeRepairState.UNREPAIRED)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isFalse();
        assertThat(repairStateSnapshot.getRepairGroups()).isEmpty();
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastCompletedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }

    @Test
    public void testCanRepairTrue()
    {
        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroups(Collections.singletonList(mockRepairGroup))
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastCompletedAt(VnodeRepairState.UNREPAIRED)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isTrue();
        assertThat(repairStateSnapshot.getRepairGroups()).containsExactly(mockRepairGroup);
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastCompletedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }

    @Test
    public void testDifferentRepairedAt()
    {
        RepairStateSnapshot repairStateSnapshot = RepairStateSnapshot.newBuilder()
                .withReplicaRepairGroups(Collections.emptyList())
                .withVnodeRepairStates(mockVnodeRepairStates)
                .withLastCompletedAt(1234L)
                .build();

        assertThat(repairStateSnapshot.canRepair()).isFalse();
        assertThat(repairStateSnapshot.getRepairGroups()).isEmpty();
        assertThat(repairStateSnapshot.getVnodeRepairStates()).isEqualTo(mockVnodeRepairStates);
        assertThat(repairStateSnapshot.lastCompletedAt()).isEqualTo(1234L);
    }
}
