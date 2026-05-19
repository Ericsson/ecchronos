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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJmxHostResolver
{
    @Mock
    private Node mockNode;

    @Mock
    private IpTranslator mockIpTranslator;

    @Test
    public void testResolvesFromBroadcastRpcAddress() throws Exception
    {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("10.0.0.1"), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(address));
        when(mockIpTranslator.isActive()).thenReturn(false);

        JmxHostResolver resolver = new JmxHostResolver(mockIpTranslator, false);
        assertThat(resolver.resolve(mockNode)).isEqualTo("10.0.0.1");
    }

    @Test
    public void testFallsBackToListenAddressWhenBroadcastIsZero() throws Exception
    {
        InetSocketAddress broadcastAddr = new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 9042);
        InetSocketAddress listenAddr = new InetSocketAddress(InetAddress.getByName("192.168.1.5"), 7000);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(broadcastAddr));
        when(mockNode.getListenAddress()).thenReturn(Optional.of(listenAddr));
        when(mockIpTranslator.isActive()).thenReturn(false);

        JmxHostResolver resolver = new JmxHostResolver(mockIpTranslator, false);
        assertThat(resolver.resolve(mockNode)).isEqualTo("192.168.1.5");
    }

    @Test
    public void testAppliesIpTranslation() throws Exception
    {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("10.0.0.1"), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(address));
        when(mockIpTranslator.isActive()).thenReturn(true);
        when(mockIpTranslator.getInternalIp("10.0.0.1")).thenReturn("172.16.0.1");

        JmxHostResolver resolver = new JmxHostResolver(mockIpTranslator, false);
        assertThat(resolver.resolve(mockNode)).isEqualTo("172.16.0.1");
    }

    @Test
    public void testWrapsIpv6InBrackets() throws Exception
    {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(address));
        when(mockIpTranslator.isActive()).thenReturn(false);

        JmxHostResolver resolver = new JmxHostResolver(mockIpTranslator, false);
        String resolved = resolver.resolve(mockNode);
        assertThat(resolved).startsWith("[").endsWith("]");
    }

    @Test
    public void testDoesNotDoubleWrapIpv6() throws Exception
    {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("::1"), 9042);
        when(mockNode.getBroadcastRpcAddress()).thenReturn(Optional.of(address));
        when(mockIpTranslator.isActive()).thenReturn(false);

        JmxHostResolver resolver = new JmxHostResolver(mockIpTranslator, false);
        String resolved = resolver.resolve(mockNode);
        // Should only have one set of brackets
        assertThat(resolved.chars().filter(c -> c == '[').count()).isEqualTo(1);
    }
}
