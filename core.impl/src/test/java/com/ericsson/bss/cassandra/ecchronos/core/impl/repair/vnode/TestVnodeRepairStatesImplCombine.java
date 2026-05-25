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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeRepairStatesImplCombine
{
    @Mock
    DriverNode mockNode;

    @Test
    public void testCombineWithOlderTimestampReturnsSelf()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 5000L);
        VnodeRepairState vnode2 = new VnodeRepairState(new LongTokenRange(100, 200),
                ImmutableSet.of(mockNode), 6000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnode1, vnode2)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(4000L);

        assertThat(result).isSameAs(states);
    }

    @Test
    public void testCombineWithEqualTimestampReturnsSelf()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 5000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnode1)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(5000L);

        assertThat(result).isSameAs(states);
    }

    @Test
    public void testCombineWithNewerTimestampUpdatesAll()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);
        VnodeRepairState vnode2 = new VnodeRepairState(new LongTokenRange(100, 200),
                ImmutableSet.of(mockNode), 2000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Arrays.asList(vnode1, vnode2)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(9000L);

        assertThat(result).isNotSameAs(states);
        for (VnodeRepairState v : result.getVnodeRepairStates())
        {
            assertThat(v.lastRepairedAt()).isEqualTo(9000L);
        }
    }

    @Test
    public void testCombineWithMixedTimestampsUpdatesOnlyOlder()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);
        VnodeRepairState vnode2 = new VnodeRepairState(new LongTokenRange(100, 200),
                ImmutableSet.of(mockNode), 5000L);
        VnodeRepairState vnode3 = new VnodeRepairState(new LongTokenRange(200, 300),
                ImmutableSet.of(mockNode), 2000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(
                Arrays.asList(vnode1, vnode2, vnode3)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(3000L);

        List<VnodeRepairState> resultList = new ArrayList<>(result.getVnodeRepairStates());
        assertThat(resultList.get(0).lastRepairedAt()).isEqualTo(3000L);
        assertThat(resultList.get(1).lastRepairedAt()).isEqualTo(5000L);
        assertThat(resultList.get(2).lastRepairedAt()).isEqualTo(3000L);
    }

    @Test
    public void testCombinePreservesOrder()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);
        VnodeRepairState vnode2 = new VnodeRepairState(new LongTokenRange(100, 200),
                ImmutableSet.of(mockNode), 2000L);
        VnodeRepairState vnode3 = new VnodeRepairState(new LongTokenRange(200, 300),
                ImmutableSet.of(mockNode), 3000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(
                Arrays.asList(vnode1, vnode2, vnode3)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(5000L);

        List<VnodeRepairState> resultList = new ArrayList<>(result.getVnodeRepairStates());
        assertThat(resultList.get(0).getTokenRange()).isEqualTo(new LongTokenRange(0, 100));
        assertThat(resultList.get(1).getTokenRange()).isEqualTo(new LongTokenRange(100, 200));
        assertThat(resultList.get(2).getTokenRange()).isEqualTo(new LongTokenRange(200, 300));
    }

    @Test
    public void testCombineWithEmptyStatesReturnsSelf()
    {
        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.emptyList()).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(5000L);

        assertThat(result).isSameAs(states);
    }

    @Test
    public void testCombineWithUnrepairedTimestampReturnsSelf()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnode1)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(VnodeRepairState.UNREPAIRED);

        assertThat(result).isSameAs(states);
    }

    @Test
    public void testCombinePreservesReplicas()
    {
        VnodeRepairState vnode1 = new VnodeRepairState(new LongTokenRange(0, 100),
                ImmutableSet.of(mockNode), 1000L);

        VnodeRepairStatesImpl states = VnodeRepairStatesImpl.newBuilder(Collections.singletonList(vnode1)).build();

        VnodeRepairStatesImpl result = states.combineWithRepairedAt(5000L);

        List<VnodeRepairState> resultList = new ArrayList<>(result.getVnodeRepairStates());
        assertThat(resultList.get(0).getReplicas()).isEqualTo(ImmutableSet.of(mockNode));
    }
}
