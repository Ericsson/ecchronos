/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeState
{
    @Mock
    private DriverNode myLocalNode;

    @Mock
    private DriverNode myRemoteNode;

    private InetAddress myLocalNodeAddress;
    private InetAddress myRemoteNodeAddress;

    @Before
    public void init() throws UnknownHostException
    {
        myLocalNodeAddress = InetAddress.getByName("127.0.0.2");
        myRemoteNodeAddress = InetAddress.getByName("127.0.0.1");
        when(myLocalNode.getPublicAddress()).thenReturn(myLocalNodeAddress);
        when(myRemoteNode.getPublicAddress()).thenReturn(myRemoteNodeAddress);
    }

    @Test
    public void testRepairedVnodeState()
    {
        long repairedAfter = 1234;
        long repairedAt = 1235;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> replicas = ImmutableSet.of(myLocalNode, myRemoteNode);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactlyInAnyOrder(myLocalNodeAddress.getHostAddress(), myRemoteNodeAddress.getHostAddress());
        assertThat(vnodeState.repaired).isTrue();
    }

    @Test
    public void testNotRepairedVnodeState()
    {
        long repairedAfter = 1235;
        long repairedAt = 1234;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> replicas = ImmutableSet.of(myLocalNode, myRemoteNode);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactlyInAnyOrder(myLocalNodeAddress.getHostAddress(), myRemoteNodeAddress.getHostAddress());
        assertThat(vnodeState.repaired).isFalse();
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.simple().forClass(VirtualNodeState.class).usingGetClass().withNonnullFields("replicas").verify();
    }
}
