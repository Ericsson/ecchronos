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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicatedTableProviderImpl
{
    private static final String LOCAL_DATACENTER = "DC1";

    @Mock
    private Metadata myMetadata;

    @Mock
    private CqlSession myCqlSession;

    private TableReferenceFactory myTableReferenceFactory = new MockTableReferenceFactory();

    private Map<CqlIdentifier, KeyspaceMetadata> myKeyspaces = new HashMap<>();

    private ReplicatedTableProviderImpl myReplicatedTableProviderImpl;

    @Before
    public void init()
    {
        Node localNode = mockNode(LOCAL_DATACENTER);

        when(myMetadata.getKeyspaces()).thenReturn(myKeyspaces);
        when(myCqlSession.getMetadata()).thenReturn(myMetadata);
        myReplicatedTableProviderImpl = new ReplicatedTableProviderImpl(localNode, myCqlSession,
                myTableReferenceFactory);
    }

    @Test
    public void testAcceptSimpleNonReplicatedKeyspaceSingleDC()
    {
        mockKeyspace("user_keyspace", simpleStrategy(1), "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isFalse();
    }

    @Test
    public void testAcceptNetworkTopologyNonReplicatedKeyspaceSingleDC()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "1");

        mockKeyspace("user_keyspace", replication, "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isFalse();
    }

    @Test
    public void testAcceptSimpleReplicatedKeyspaceSingleDC()
    {
        mockKeyspace("user_keyspace", simpleStrategy(3), "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isTrue();
    }

    @Test
    public void testAcceptNetworkTopologyReplicatedKeyspaceSingleDC()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "3");

        mockKeyspace("user_keyspace", replication, "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isTrue();
    }

    @Test
    public void testAcceptSimpleReplicatedKeyspaceMultipleDC()
    {
        mockKeyspace("user_keyspace", simpleStrategy(3), "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isTrue();
    }

    @Test
    public void testAcceptNetworkTopologyReplicatedKeyspaceMultipleDC()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "1");
        replication.put("DC2", "1");
        replication.put("DC3", "1");

        mockKeyspace("user_keyspace", replication, "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isTrue();
    }

    @Test
    public void testAcceptNonLocallyReplicatedTable()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC2", "1");
        replication.put("DC3", "1");

        mockKeyspace("user_keyspace", replication, "table1");

        assertThat(myReplicatedTableProviderImpl.accept("user_keyspace")).isFalse();
    }

    @Test
    public void testAcceptLocalNodeDCIsUnavailableNetworkTopology()
    {
        Node localNode = mockNode(null);

        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "3");

        mockKeyspace("user_keyspace", replication, "table1");

        ReplicatedTableProviderImpl replicatedTableProviderImpl = new ReplicatedTableProviderImpl(localNode,
                myCqlSession,
                myTableReferenceFactory);
        assertThat(replicatedTableProviderImpl.accept("user_keyspace")).isFalse();
    }

    @Test
    public void testAcceptLocalNodeDCIsUnavailableSimple()
    {
        Node localNode = mockNode(null);
        mockKeyspace("user_keyspace", simpleStrategy(3), "table1");

        ReplicatedTableProviderImpl replicatedTableProviderImpl = new ReplicatedTableProviderImpl(localNode,
                myCqlSession,
                myTableReferenceFactory);
        assertThat(replicatedTableProviderImpl.accept("user_keyspace")).isTrue();
    }

    @Test
    public void testGetAllSingleDCNoSystemOnlyKeyspaces()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "3");

        mockKeyspace("system_auth", replication, "roles", "role_members", "role_permissions");
        mockKeyspace("system_distributed", replication, "parent_repair_history", "repair_history");
        mockKeyspace("user_keyspace", replication, "table1", "table2");

        TableReference[] expectedTableReferences = new TableReference[] {
                tableReference("system_auth", "roles"),
                tableReference("system_auth", "role_members"),
                tableReference("system_auth", "role_permissions"),
                tableReference("user_keyspace", "table1"),
                tableReference("user_keyspace", "table2")
        };

        assertThat(myReplicatedTableProviderImpl.getAll()).containsExactlyInAnyOrder(expectedTableReferences);
    }

    @Test
    public void testGetAllMultipleDCNoSystemOnlyKeyspaces()
    {
        Map<String, String> replication = networkTopologyStrategy();
        replication.put("DC1", "1");
        replication.put("DC2", "1");
        replication.put("DC3", "1");

        mockKeyspace("system_auth", replication, "roles", "role_members", "role_permissions");
        mockKeyspace("system_distributed", replication, "parent_repair_history", "repair_history");
        mockKeyspace("user_keyspace", replication, "table1", "table2");

        TableReference[] expectedTableReferences = new TableReference[] {
                tableReference("system_auth", "roles"),
                tableReference("system_auth", "role_members"),
                tableReference("system_auth", "role_permissions"),
                tableReference("user_keyspace", "table1"),
                tableReference("user_keyspace", "table2")
        };

        assertThat(myReplicatedTableProviderImpl.getAll()).containsExactlyInAnyOrder(expectedTableReferences);
    }

    @Test
    public void testAcceptUnknownKeyspace()
    {
        assertThat(myReplicatedTableProviderImpl.accept("nonexistingkeyspace")).isFalse();
    }

    private Map<String, String> simpleStrategy(int replicationFactor)
    {
        Map<String, String> replication = new HashMap<>();
        replication.put("class", "org.apache.cassandra.locator.SimpleStrategy");
        replication.put("replication_factor", Integer.toString(replicationFactor));

        return replication;
    }

    private Map<String, String> networkTopologyStrategy()
    {
        Map<String, String> replication = new HashMap<>();
        replication.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        return replication;
    }

    private Node mockNode(String dataCenter)
    {
        Node node = mock(Node.class);

        when(node.getDatacenter()).thenReturn(dataCenter);

        return node;
    }

    private void mockKeyspace(String keyspace, Map<String, String> replication, String... tables)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);

        when(keyspaceMetadata.getName()).thenReturn(CqlIdentifier.fromCql(keyspace));

        Map<CqlIdentifier, TableMetadata> tableMetadatas = new HashMap<>();
        for (String table : tables)
        {
            TableMetadata tableMetadata = mock(TableMetadata.class);

            when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromCql(table));
            when(tableMetadata.getKeyspace()).thenReturn(CqlIdentifier.fromCql(keyspace));

            tableMetadatas.put(CqlIdentifier.fromCql(table), tableMetadata);
        }

        when(keyspaceMetadata.getTables()).thenReturn(tableMetadatas);

        myKeyspaces.put(CqlIdentifier.fromCql(keyspace), keyspaceMetadata);
        when(myMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));

        when(keyspaceMetadata.getReplication()).thenReturn(replication);
    }
}
