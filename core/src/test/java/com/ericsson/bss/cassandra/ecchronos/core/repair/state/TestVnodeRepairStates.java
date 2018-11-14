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

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestVnodeRepairStates
{
    @Test
    public void testVnodeRepairStates()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Host host1 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, Sets.newHashSet(host1), VnodeRepairState.UNREPAIRED);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(range2, Sets.newHashSet(host1), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairStates(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .build();

        assertThat(vnodeRepairStates.getVnodeRepairStates()).containsExactlyInAnyOrder(vnodeRepairState, vnodeRepairState2);
    }

    @Test
    public void testCombineMoreRecentlyRepaired()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Host host1 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, Sets.newHashSet(host1), VnodeRepairState.UNREPAIRED);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range, Sets.newHashSet(host1), 1234L);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(vnodeRepairState)
                .combineVnodeRepairState(updatedVnodeRepairState)
                .combineVnodeRepairState(vnodeRepairState)
                .build();

        assertThat(vnodeRepairStates.getVnodeRepairStates()).containsExactly(updatedVnodeRepairState);
    }

    @Test
    public void testCombineNotSame()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, Sets.newHashSet(host1, host2), 1234L);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range, Sets.newHashSet(host1, host3), VnodeRepairState.UNREPAIRED);

        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(vnodeRepairState)
                .combineVnodeRepairState(updatedVnodeRepairState)
                .build();

        assertThat(vnodeRepairStates.getVnodeRepairStates()).containsExactly(updatedVnodeRepairState);
    }
}
