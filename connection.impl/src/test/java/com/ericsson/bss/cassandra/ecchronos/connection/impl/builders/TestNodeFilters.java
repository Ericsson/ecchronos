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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestNodeFilters
{
    @Mock
    private CqlSession mockSession;
    @Mock
    private Metadata mockMetadata;
    @Mock
    private Node nodeA;
    @Mock
    private Node nodeB;

    @Before
    public void setup()
    {
        when(nodeA.getDatacenter()).thenReturn("dc1");
        when(nodeA.getRack()).thenReturn("rack1");
        when(nodeA.getEndPoint()).thenReturn(new ContactEndPoint("10.0.0.1", 9042));

        when(nodeB.getDatacenter()).thenReturn("dc2");
        when(nodeB.getRack()).thenReturn("rack2");
        when(nodeB.getEndPoint()).thenReturn(new ContactEndPoint("10.0.0.2", 9042));

        Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(UUID.randomUUID(), nodeA);
        nodes.put(UUID.randomUUID(), nodeB);
        when(mockMetadata.getNodes()).thenReturn(nodes);
        when(mockSession.getMetadata()).thenReturn(mockMetadata);
    }

    @Test
    public void testDatacenterFilterEmptyListMatchesNothing()
    {
        DatacenterNodeFilter filter = new DatacenterNodeFilter(Collections.emptyList());
        assertThat(filter.resolve(mockSession)).isEmpty();
        assertThat(filter.isValid(nodeA)).isFalse();
    }

    @Test
    public void testDatacenterFilterMultipleDatacenters()
    {
        DatacenterNodeFilter filter = new DatacenterNodeFilter(List.of("dc1", "dc2"));
        assertThat(filter.resolve(mockSession)).hasSize(2);
        assertThat(filter.isValid(nodeA)).isTrue();
        assertThat(filter.isValid(nodeB)).isTrue();
    }

    @Test
    public void testRackFilterEmptyListMatchesNothing()
    {
        RackNodeFilter filter = new RackNodeFilter(Collections.emptyList());
        assertThat(filter.resolve(mockSession)).isEmpty();
        assertThat(filter.isValid(nodeA)).isFalse();
    }

    @Test
    public void testRackFilterWrongRackInCorrectDc()
    {
        Map<String, String> rack = new HashMap<>();
        rack.put("datacenterName", "dc1");
        rack.put("rackName", "rack2");
        RackNodeFilter filter = new RackNodeFilter(List.of(rack));
        assertThat(filter.isValid(nodeA)).isFalse();
        assertThat(filter.isValid(nodeB)).isFalse();
    }

    @Test
    public void testHostFilterEmptyListMatchesNothing()
    {
        HostNodeFilter filter = new HostNodeFilter(Collections.emptyList());
        assertThat(filter.resolve(mockSession)).isEmpty();
        assertThat(filter.isValid(nodeA)).isFalse();
    }

    @Test
    public void testHostFilterWrongPortDoesNotMatch()
    {
        HostNodeFilter filter = new HostNodeFilter(List.of(new InetSocketAddress("10.0.0.1", 7000)));
        assertThat(filter.isValid(nodeA)).isFalse();
    }

    @Test
    public void testHostFilterExactMatch()
    {
        HostNodeFilter filter = new HostNodeFilter(List.of(new InetSocketAddress("10.0.0.1", 9042)));
        assertThat(filter.isValid(nodeA)).isTrue();
        assertThat(filter.isValid(nodeB)).isFalse();
    }
}
