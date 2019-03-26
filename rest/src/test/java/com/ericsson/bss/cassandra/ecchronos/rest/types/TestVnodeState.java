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
package com.ericsson.bss.cassandra.ecchronos.rest.types;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeState
{
    @Mock
    private Host myLocalHost;

    @Mock
    private Host myRemoteHost;

    private InetAddress myLocalHostAddress;
    private InetAddress myRemoteHostAddress;

    @Before
    public void init() throws UnknownHostException
    {
        myLocalHostAddress = InetAddress.getByName("127.0.0.2");
        myRemoteHostAddress = InetAddress.getByName("127.0.0.1");
        when(myLocalHost.getBroadcastAddress()).thenReturn(myLocalHostAddress);
        when(myRemoteHost.getBroadcastAddress()).thenReturn(myRemoteHostAddress);
    }

    @Test
    public void testRepairedVnodeState()
    {
        long repairedAfter = 1234;
        long repairedAt = 1235;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        Collection<Host> replicas = Arrays.asList(myLocalHost, myRemoteHost);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactlyInAnyOrder(myLocalHostAddress, myRemoteHostAddress);
        assertThat(vnodeState.repaired).isTrue();
    }

    @Test
    public void testNotRepairedVnodeState()
    {
        long repairedAfter = 1235;
        long repairedAt = 1234;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        Collection<Host> replicas = Arrays.asList(myLocalHost, myRemoteHost);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactlyInAnyOrder(myLocalHostAddress, myRemoteHostAddress);
        assertThat(vnodeState.repaired).isFalse();
    }
}
