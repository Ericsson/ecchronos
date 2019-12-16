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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestVnodeRepairState
{
    @Test
    public void testVnodeRepairState()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1, host2, host3), VnodeRepairState.UNREPAIRED);

        assertThat(vnodeRepairState.getReplicas()).containsExactlyInAnyOrder(host1, host2, host3);
        assertThat(vnodeRepairState.getTokenRange()).isEqualTo(range);
        assertThat(vnodeRepairState.lastRepairedAt()).isEqualTo(VnodeRepairState.UNREPAIRED);
        assertThat(vnodeRepairState.isSameVnode(vnodeRepairState)).isTrue();
    }

    @Test
    public void testVnodeRepairStateRepairedAtIsSet()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);
        long repairedAt = 1234L;

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1, host2, host3), repairedAt);

        assertThat(vnodeRepairState.lastRepairedAt()).isEqualTo(repairedAt);
    }

    @Test
    public void testVnodeWithDifferentReplicasIsNotSame()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1, host2), VnodeRepairState.UNREPAIRED);
        VnodeRepairState otherVnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1, host3), VnodeRepairState.UNREPAIRED);

        assertThat(vnodeRepairState.isSameVnode(otherVnodeRepairState)).isFalse();
    }

    @Test
    public void testDifferentVnodesAreNotSame()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange otherRange = new LongTokenRange(2, 3);
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(host1, host2, host3), VnodeRepairState.UNREPAIRED);
        VnodeRepairState otherVnodeRepairState = new VnodeRepairState(otherRange, ImmutableSet.of(host1, host2, host3), VnodeRepairState.UNREPAIRED);

        assertThat(vnodeRepairState.isSameVnode(otherVnodeRepairState)).isFalse();
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(VnodeRepairState.class)
                .withPrefabValues(ImmutableSet.class, ImmutableSet.of(1), ImmutableSet.of(2))
                .usingGetClass()
                .verify();
    }
}
