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
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeState
{
    @Mock
    private Host myHost;

    private InetAddress myLocalHost;

    @Before
    public void init() throws UnknownHostException
    {
        myLocalHost = InetAddress.getLocalHost();
        when(myHost.getBroadcastAddress()).thenReturn(myLocalHost);
    }

    @Test
    public void testRepairedVnodeState()
    {
        long repairedAfter = 1234;
        long repairedAt = 1235;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        Collection<Host> replicas = Collections.singletonList(myHost);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VnodeState vnodeState = VnodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAt).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactly(myLocalHost);
        assertThat(vnodeState.repaired).isTrue();
    }

    @Test
    public void testNotRepairedVnodeState()
    {
        long repairedAfter = 1235;
        long repairedAt = 1234;
        LongTokenRange tokenRange = new LongTokenRange(1, 2);
        Collection<Host> replicas = Collections.singletonList(myHost);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(tokenRange, replicas, repairedAt);
        VnodeState vnodeState = VnodeState.convert(vnodeRepairState, repairedAfter);

        assertThat(vnodeState.startToken).isEqualTo(1);
        assertThat(vnodeState.endToken).isEqualTo(2);
        assertThat(vnodeState.lastRepairedAt).isEqualTo(repairedAt);
        assertThat(vnodeState.replicas).containsExactly(myLocalHost);
        assertThat(vnodeState.repaired).isFalse();
    }
}
