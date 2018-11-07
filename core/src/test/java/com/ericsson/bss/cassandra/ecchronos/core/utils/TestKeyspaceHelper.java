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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

@RunWith(MockitoJUnitRunner.class)
public class TestKeyspaceHelper
{
    private static final String keyspaceName = "keyspace";

    @Mock
    private Metadata metadata;

    @Mock
    private KeyspaceMetadata keyspaceMetadata;

    private Map<String, String> replicationMap = new HashMap<>();

    @Before
    public void init()
    {
        when(metadata.getKeyspace(eq(keyspaceName))).thenReturn(keyspaceMetadata);
        when(keyspaceMetadata.getReplication()).thenReturn(replicationMap);
    }

    @Test
    public void testGetDatacentersWithNetworkTopologyStrategy()
    {
        replicationMap.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationMap.put("dc1", "3");
        replicationMap.put("dc2", "3");

        Collection<String> datacenters = KeyspaceHelper.getDatacentersForKeyspace(metadata, keyspaceName);

        assertThat(datacenters).containsOnly("dc1", "dc2");
    }

    @Test
    public void testGetDatacentersWithSimpleStrategy()
    {
        replicationMap.put("class", "org.apache.cassandra.locator.SimpleStrategy");
        replicationMap.put("replication_factor", "3");
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Set<Host> hosts = new HashSet<>();
        hosts.add(host1);
        hosts.add(host2);

        when(metadata.getAllHosts()).thenReturn(hosts);
        when(host1.getDatacenter()).thenReturn("dc1");
        when(host2.getDatacenter()).thenReturn("dc2");

        Collection<String> datacenters = KeyspaceHelper.getDatacentersForKeyspace(metadata, keyspaceName);

        assertThat(datacenters).containsOnly("dc1", "dc2");
    }

    @Test
    public void testGetDatacenters()
    {
        Host host1 = mock(Host.class);
        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);
        Set<Host> hosts = new HashSet<>();
        hosts.add(host1);
        hosts.add(host2);
        hosts.add(host3);

        when(metadata.getAllHosts()).thenReturn(hosts);
        when(host1.getDatacenter()).thenReturn("dc1");
        when(host2.getDatacenter()).thenReturn("dc2");
        when(host3.getDatacenter()).thenReturn("dc3");

        Collection<String> datacenters = KeyspaceHelper.getDatacenters(metadata);

        assertThat(datacenters).containsOnly("dc1", "dc2", "dc3");
    }
}
