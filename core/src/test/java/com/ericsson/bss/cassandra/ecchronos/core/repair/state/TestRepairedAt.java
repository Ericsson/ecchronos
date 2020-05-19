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
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestRepairedAt
{
    @Test
    public void testRepaired()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Host host1 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1), 1234L);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(range2, ImmutableSet.of(host1), 1235L);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .build();

        RepairedAt repairedAt = RepairedAt.generate(vnodeRepairStates);

        assertThat(repairedAt.isRepaired()).isTrue();
        assertThat(repairedAt.isPartiallyRepaired()).isFalse();
        assertThat(repairedAt.getMaxRepairedAt()).isEqualTo(1235L);
        assertThat(repairedAt.getMinRepairedAt()).isEqualTo(1234L);
    }

    @Test
    public void testPartiallyRepaired()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Host host1 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1), 1234L);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(range2, ImmutableSet.of(host1), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .build();

        RepairedAt repairedAt = RepairedAt.generate(vnodeRepairStates);

        assertThat(repairedAt.isRepaired()).isFalse();
        assertThat(repairedAt.isPartiallyRepaired()).isTrue();
        assertThat(repairedAt.getMaxRepairedAt()).isEqualTo(1234L);
        assertThat(repairedAt.getMinRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }

    @Test
    public void testNotRepaired()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Host host1 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1), VnodeRepairState.UNREPAIRED);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(range2, ImmutableSet.of(host1), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .build();

        RepairedAt repairedAt = RepairedAt.generate(vnodeRepairStates);

        assertThat(repairedAt.isRepaired()).isFalse();
        assertThat(repairedAt.isPartiallyRepaired()).isFalse();
        assertThat(repairedAt.getMaxRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
        assertThat(repairedAt.getMinRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
    }
}
