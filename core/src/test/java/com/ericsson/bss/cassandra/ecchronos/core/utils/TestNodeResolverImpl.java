/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeResolverImpl
{
    @Mock
    private Metadata mockMetadata;

    private Set<Host> mockedHosts = new HashSet<>();

    private NodeResolver nodeResolver;

    @Before
    public void setup() throws Exception
    {
        when(mockMetadata.getAllHosts()).thenReturn(mockedHosts);

        // Add two dummy hosts so that we know we find the correct host
        addHost(address("127.0.0.2"), "dc1");
        addHost(address("127.0.0.3"), "dc1");

        nodeResolver = new NodeResolverImpl(mockMetadata);
    }

    @Test
    public void testGetHost() throws Exception
    {
        Host host = addHost(address("127.0.0.1"), "dc1");

        Optional<Node> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();
        Node node = maybeNode.get();
        assertThat(node.getId()).isEqualTo(host.getHostId());
        assertThat(node.getPublicAddress()).isEqualTo(address("127.0.0.1"));
        assertThat(node.getDatacenter()).isEqualTo("dc1");

        assertThat(nodeResolver.fromIp(address("127.0.0.1"))).containsSame(node);
    }

    @Test
    public void testChangeIpAddress() throws Exception
    {
        Host host = addHost(address("127.0.0.1"), "dc1");

        Optional<Node> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();
        Node node = maybeNode.get();

        assertThat(node.getPublicAddress()).isEqualTo(address("127.0.0.1"));

        when(host.getBroadcastAddress()).thenReturn(address("127.0.0.5"));

        assertThat(node.getId()).isEqualTo(host.getHostId());
        assertThat(node.getPublicAddress()).isEqualTo(address("127.0.0.5"));
        assertThat(node.getDatacenter()).isEqualTo("dc1");

        // New mapping for the node
        assertThat(nodeResolver.fromIp(address("127.0.0.5"))).containsSame(node);

        // Make sure the old mapping is removed
        assertThat(nodeResolver.fromIp(address("127.0.0.1"))).isEmpty();
    }

    @Test
    public void testChangeIpAddressAndAddNewReplica() throws Exception
    {
        Host host = addHost(address("127.0.0.1"), "dc1");

        Optional<Node> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();
        Node node = maybeNode.get();

        assertThat(node.getPublicAddress()).isEqualTo(address("127.0.0.1"));

        when(host.getBroadcastAddress()).thenReturn(address("127.0.0.5"));

        assertThat(node.getId()).isEqualTo(host.getHostId());
        assertThat(node.getPublicAddress()).isEqualTo(address("127.0.0.5"));
        assertThat(node.getDatacenter()).isEqualTo("dc1");

        // New mapping for the node
        assertThat(nodeResolver.fromIp(address("127.0.0.5"))).containsSame(node);

        // If a new node is using the old ip we should return it
        Host newHost = addHost(address("127.0.0.1"), "dc2");

        Optional<Node> maybeNewNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNewNode).isPresent();
        Node newNode = maybeNewNode.get();

        assertThat(newNode.getId()).isEqualTo(newHost.getHostId());
        assertThat(newNode.getPublicAddress()).isEqualTo(address("127.0.0.1"));
        assertThat(newNode.getDatacenter()).isEqualTo("dc2");
        assertThat(newNode).isNotSameAs(node);
    }

    @Test
    public void testGetNonExistingHost() throws Exception
    {
        Optional<Node> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isEmpty();
    }

    private InetAddress address(String address) throws UnknownHostException
    {
        return InetAddress.getByName(address);
    }

    private Host addHost(InetAddress broadcastAddress, String dataCenter)
    {
        Host host = mock(Host.class);

        when(host.getHostId()).thenReturn(UUID.randomUUID());
        when(host.getBroadcastAddress()).thenReturn(broadcastAddress);
        when(host.getDatacenter()).thenReturn(dataCenter);

        mockedHosts.add(host);
        when(mockMetadata.getAllHosts()).thenReturn(mockedHosts);
        return host;
    }
}
