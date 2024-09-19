/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeResolverImpl
{
    @Mock
    private Metadata mockMetadata;

    @Mock
    private CqlSession mockCqlSession;

    private Map<UUID, Node> mockedNodes = new HashMap<>();

    private NodeResolver nodeResolver;

    @Before
    public void setup() throws Exception
    {
        when(mockMetadata.getNodes()).thenReturn(mockedNodes);

        // Add two dummy hosts so that we know we find the correct host
        addNode(new InetSocketAddress(address("127.0.0.2"), 9042), "dc1");
        addNode(new InetSocketAddress(address("127.0.0.3"), 9042), "dc1");

        when(mockCqlSession.getMetadata()).thenReturn(mockMetadata);

        nodeResolver = new NodeResolverImpl(mockCqlSession);
    }

    @Test
    public void testGetHost() throws Exception
    {
        Node node = addNode(new InetSocketAddress(address("127.0.0.1"), 9042), "dc1");

        Optional<DriverNode> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();
        assertThat(maybeNode.get().getId()).isEqualTo(node.getHostId());
        assertThat(maybeNode.get().getPublicAddress()).isEqualTo(address("127.0.0.1"));
        assertThat(maybeNode.get().getDatacenter()).isEqualTo("dc1");

        assertThat(nodeResolver.fromIp(address("127.0.0.1"))).containsSame(maybeNode.get());
        assertThat(nodeResolver.fromUUID(node.getHostId())).containsSame(maybeNode.get());
    }

    @Test
    public void testChangeIpAddress() throws Exception
    {
        Node node = addNode(new InetSocketAddress(address("127.0.0.1"), 9042), "dc1");

        Optional<DriverNode> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();

        assertThat(maybeNode.get().getPublicAddress()).isEqualTo(address("127.0.0.1"));

        when(node.getBroadcastAddress()).thenReturn(Optional.of(new InetSocketAddress(address("127.0.0.5"), 9042)));

        assertThat(maybeNode.get().getId()).isEqualTo(node.getHostId());
        assertThat(maybeNode.get().getPublicAddress()).isEqualTo(address("127.0.0.5"));
        assertThat(maybeNode.get().getDatacenter()).isEqualTo("dc1");

        // New mapping for the node
        assertThat(nodeResolver.fromIp(address("127.0.0.5"))).containsSame(maybeNode.get());
        assertThat(nodeResolver.fromUUID(node.getHostId())).containsSame(maybeNode.get());

        // Make sure the old mapping is removed
        assertThat(nodeResolver.fromIp(address("127.0.0.1"))).isEmpty();
    }

    @Test
    public void testChangeIpAddressAndAddNewReplica() throws Exception
    {
        Node node = addNode(new InetSocketAddress(address("127.0.0.1"), 9042), "dc1");

        Optional<DriverNode> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isPresent();

        assertThat(maybeNode.get().getPublicAddress()).isEqualTo(address("127.0.0.1"));

        when(node.getBroadcastAddress()).thenReturn(Optional.of(new InetSocketAddress(address("127.0.0.5"), 9042)));

        assertThat(maybeNode.get().getId()).isEqualTo(node.getHostId());
        assertThat(maybeNode.get().getPublicAddress()).isEqualTo(address("127.0.0.5"));
        assertThat(maybeNode.get().getDatacenter()).isEqualTo("dc1");

        // New mapping for the node
        assertThat(nodeResolver.fromIp(address("127.0.0.5"))).containsSame(maybeNode.get());
        assertThat(nodeResolver.fromUUID(node.getHostId())).containsSame(maybeNode.get());

        // If a new node is using the old ip we should return it
        Node newNode = addNode(new InetSocketAddress(address("127.0.0.1"), 9042), "dc2");

        Optional<DriverNode> maybeNewNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNewNode).isPresent();

        assertThat(maybeNewNode.get().getId()).isEqualTo(newNode.getHostId());
        assertThat(maybeNewNode.get().getPublicAddress()).isEqualTo(address("127.0.0.1"));
        assertThat(maybeNewNode.get().getDatacenter()).isEqualTo("dc2");
        assertThat(nodeResolver.fromUUID(newNode.getHostId())).containsSame(maybeNewNode.get());

        assertThat(maybeNewNode.get()).isNotSameAs(maybeNode.get());
    }

    @Test
    public void testGetNonExistingHost() throws Exception
    {
        Optional<DriverNode> maybeNode = nodeResolver.fromIp(address("127.0.0.1"));
        assertThat(maybeNode).isEmpty();

        maybeNode = nodeResolver.fromUUID(UUID.randomUUID());
        assertThat(maybeNode).isEmpty();
    }

    private InetAddress address(String address) throws UnknownHostException
    {
        return InetAddress.getByName(address);
    }

    private Node addNode(InetSocketAddress broadcastAddress, String dataCenter)
    {
        Node node = mock(Node.class);

        UUID id = UUID.randomUUID();
        when(node.getHostId()).thenReturn(id);
        when(node.getBroadcastAddress()).thenReturn(Optional.of(broadcastAddress));
        when(node.getDatacenter()).thenReturn(dataCenter);

        mockedNodes.put(id, node);
        when(mockMetadata.getNodes()).thenReturn(mockedNodes);
        return node;
    }
}
