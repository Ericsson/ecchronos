/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestBuilderPresize
{
    @Mock
    DriverNode mockNode;

    @Test
    public void testVnodeBuilderEmptyCollection()
    {
        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.emptyList()).build();
        assertThat(states.getVnodeRepairStates()).isEmpty();
    }

    @Test
    public void testVnodeBuilderSingleEntry()
    {
        VnodeRepairState vnode = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnode)).build();

        assertThat(states.getVnodeRepairStates()).hasSize(1);
        assertThat(states.getVnodeRepairStates().iterator().next()).isEqualTo(vnode);
    }

    @Test
    public void testVnodeBuilderTypicalSize()
    {
        List<VnodeRepairState> vnodes = createVnodes(54);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(vnodes).build();

        assertThat(states.getVnodeRepairStates()).hasSize(54);
    }

    @Test
    public void testVnodeBuilderLargeCollection()
    {
        List<VnodeRepairState> vnodes = createVnodes(500);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(vnodes).build();

        assertThat(states.getVnodeRepairStates()).hasSize(500);
    }

    @Test
    public void testVnodeBuilderPreservesOrder()
    {
        List<VnodeRepairState> vnodes = createVnodes(10);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(vnodes).build();

        List<VnodeRepairState> result = new ArrayList<>(states.getVnodeRepairStates());
        for (int i = 0; i < 10; i++)
        {
            assertThat(result.get(i).getTokenRange()).isEqualTo(vnodes.get(i).getTokenRange());
        }
    }

    @Test
    public void testSubRangeBuilderEmptyCollection()
    {
        SubRangeRepairStates states = SubRangeRepairStates.newBuilder(Collections.emptyList()).build();
        assertThat(states.getVnodeRepairStates()).isEmpty();
    }

    @Test
    public void testSubRangeBuilderSingleEntry()
    {
        VnodeRepairState vnode = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);

        SubRangeRepairStates states = SubRangeRepairStates.newBuilder(Collections.singletonList(vnode)).build();

        assertThat(states.getVnodeRepairStates()).hasSize(1);
    }

    @Test
    public void testSubRangeBuilderTypicalSize()
    {
        List<VnodeRepairState> vnodes = createVnodes(54);

        SubRangeRepairStates states = SubRangeRepairStates.newBuilder(vnodes).build();

        assertThat(states.getVnodeRepairStates()).hasSize(54);
    }

    @Test
    public void testSubRangeBuilderLargeCollection()
    {
        List<VnodeRepairState> vnodes = createVnodes(500);

        SubRangeRepairStates states = SubRangeRepairStates.newBuilder(vnodes).build();

        assertThat(states.getVnodeRepairStates()).hasSize(500);
    }

    private List<VnodeRepairState> createVnodes(int count)
    {
        List<VnodeRepairState> vnodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            vnodes.add(new VnodeRepairState(new LongTokenRange(i * 100L, (i + 1) * 100L),
                    ImmutableSet.of(mockNode), 1000L + i));
        }
        return vnodes;
    }
}
