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
package com.ericsson.bss.cassandra.ecchronos.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.Host;

@RunWith (MockitoJUnitRunner.class)
public class TestHostStatesImpl
{
    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private JmxProxy myJmxProxy;

    private HostStatesImpl myHostStates;

    @Before
    public void setup() throws IOException
    {
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);

        myHostStates = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();
    }

    @After
    public void cleanup()
    {
        myHostStates.close();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testUseNullJmxProxyFactoryShouldThrow()
    {
        HostStatesImpl.builder()
                .withJmxProxyFactory(null)
                .build();
    }

    @Test
    public void testIsInetAddressUp() throws UnknownHostException
    {
        InetAddress expectedAddress = InetAddress.getLocalHost();

        List<String> expectedLiveNodes = Collections.singletonList(expectedAddress.getHostName());
        List<String> expectedUnreachableNodes = Collections.emptyList();

        when(myJmxProxy.getLiveNodes()).thenReturn(expectedLiveNodes);
        when(myJmxProxy.getUnreachableNodes()).thenReturn(expectedUnreachableNodes);

        assertThat(myHostStates.isUp(expectedAddress)).isTrue();
    }

    @Test
    public void testIsHostUp() throws UnknownHostException
    {
        InetAddress expectedAddress = InetAddress.getLocalHost();
        Host expectedHost = mock(Host.class);

        List<String> expectedLiveNodes = Collections.singletonList(expectedAddress.getHostName());
        List<String> expectedUnreachableNodes = Collections.emptyList();

        when(myJmxProxy.getLiveNodes()).thenReturn(expectedLiveNodes);
        when(myJmxProxy.getUnreachableNodes()).thenReturn(expectedUnreachableNodes);

        when(expectedHost.getBroadcastAddress()).thenReturn(expectedAddress);

        assertThat(myHostStates.isUp(expectedHost)).isTrue();
    }

    @Test
    public void testIsInetAddressUpFaultyNode() throws UnknownHostException
    {
        InetAddress expectedAddress = InetAddress.getLocalHost();

        when(myJmxProxy.getLiveNodes()).thenReturn(Collections.emptyList());
        when(myJmxProxy.getUnreachableNodes()).thenReturn(Collections.emptyList());

        assertThat(myHostStates.isUp(expectedAddress)).isFalse();
    }

    @Test
    public void testNodeIsNotRefreshed() throws UnknownHostException
    {
        final InetAddress expectedAddress = InetAddress.getLocalHost();

        when(myJmxProxy.getLiveNodes()).thenAnswer(new Answer<List<String>>()
        {
            private int counter = 0;

            @Override
            public List<String> answer(InvocationOnMock invocation)
            {
                if (counter++ == 2)
                {
                    return Collections.singletonList(expectedAddress.getHostAddress());
                }

                return Collections.emptyList();
            }

        });

        when(myJmxProxy.getUnreachableNodes()).thenAnswer(new Answer<List<String>>()
        {
            private int counter = 0;

            @Override
            public List<String> answer(InvocationOnMock invocation)
            {
                if (counter++ == 2)
                {
                    return Collections.emptyList();
                }

                return Collections.singletonList(expectedAddress.getHostAddress());
            }

        });

        assertThat(myHostStates.isUp(expectedAddress)).isFalse();
        assertThat(myHostStates.isUp(expectedAddress)).isFalse();
    }

    @Test
    public void testNodeIsRefreshed() throws UnknownHostException
    {
        final InetAddress expectedAddress = InetAddress.getLocalHost();

        HostStatesImpl hostStates = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withRefreshIntervalInMs(1)
                .build();

        when(myJmxProxy.getLiveNodes()).thenAnswer(new Answer<List<String>>()
        {
            private int counter = 0;

            @Override
            public List<String> answer(InvocationOnMock invocation)
            {
                if (counter++ == 1)
                {
                    return Collections.singletonList(expectedAddress.getHostAddress());
                }
                return Collections.emptyList();
            }

        });

        when(myJmxProxy.getUnreachableNodes()).thenAnswer(new Answer<List<String>>()
                {
            private int counter = 0;

            @Override
            public List<String> answer(InvocationOnMock invocation)
            {
                if (counter++ == 1)
                {
                    return Collections.emptyList();
                }
                return Collections.singletonList(expectedAddress.getHostAddress());
            }

        });

        assertThat(hostStates.isUp(expectedAddress)).isFalse();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS).until(() -> hostStates.isUp(expectedAddress));
    }
}
