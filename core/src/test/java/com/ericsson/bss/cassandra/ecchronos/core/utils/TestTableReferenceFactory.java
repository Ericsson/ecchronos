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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestTableReferenceFactory
{
    @Mock
    private Metadata mockMetadata;

    @Mock
    private CqlSession mockCqlSession;

    private TableReferenceFactory tableReferenceFactory;

    private Map<CqlIdentifier, KeyspaceMetadata> mockedKeyspaces = new HashMap<>();

    @Before
    public void setup()
    {
        when(mockCqlSession.getMetadata()).thenReturn(mockMetadata);
        tableReferenceFactory = new TableReferenceFactoryImpl(mockCqlSession);
    }

    @Test
    public void testGetKeyspaceAndTableName()
    {
        mockTable("keyspace", "table");

        TableReference tableReference = tableReferenceFactory.forTable("keyspace", "table");

        assertThat(tableReference.getKeyspace()).isEqualTo("keyspace");
        assertThat(tableReference.getTable()).isEqualTo("table");
    }

    @Test
    public void testNewTableIsNotEqual()
    {
        mockTable("keyspace", "table");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace", "table");

        mockTable("keyspace", "table"); // New table uuid
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace", "table");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
    }

    @Test
    public void testSameTableIsEqual()
    {
        mockTable("keyspace", "table");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace", "table");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace", "table");

        assertThat(tableReference1).isEqualTo(tableReference2);
    }

    @Test
    public void testDifferentKeyspacesNotEqual()
    {
        mockTable("keyspace", "table");
        mockTable("keyspace2", "table");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace", "table");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace2", "table");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isNotEqualTo(tableReference2.hashCode());
    }

    @Test
    public void testDifferentTablesNotEqual()
    {
        mockTable("keyspace", "table");
        mockTable("keyspace", "table2");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace", "table");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace", "table2");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isNotEqualTo(tableReference2.hashCode());
    }

    @Test
    public void testForKeyspace() throws EcChronosException
    {
        Set<String> tables = new HashSet<>();
        tables.add("table1");
        tables.add("table2");
        mockKeyspace("keyspace111", tables);
        mockKeyspace("keyspace222", Collections.singleton("table1"));
        Set<TableReference> tableReferences = tableReferenceFactory.forKeyspace("keyspace111");
        assertThat(tableReferences).hasSize(2);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("keyspace111"))
                .collect(Collectors.toList())).isNotEmpty();
    }

    @Test
    public void testForKeyspaceNoTables() throws EcChronosException
    {
        mockEmptyKeyspace("keyspaceEmpty");
        Set<TableReference> tableReferences = tableReferenceFactory.forKeyspace("keyspaceEmpty");
        assertThat(tableReferences).hasSize(0);
    }

    @Test (expected = EcChronosException.class)
    public void testForKeyspaceDoesNotExist() throws EcChronosException
    {
        tableReferenceFactory.forKeyspace("keyspaceEmpty");
    }

    @Test
    public void testForCluster()
    {
        Set<String> firstKsTables = new HashSet<>();
        firstKsTables.add("table1");
        firstKsTables.add("table2");
        mockKeyspace("firstks", firstKsTables);
        mockKeyspace("secondks", Collections.singleton("table1"));
        mockKeyspace("thirdks", Collections.singleton("table1"));

        when(mockMetadata.getKeyspaces()).thenReturn(mockedKeyspaces);
        Set<TableReference> tableReferences = tableReferenceFactory.forCluster();
        assertThat(tableReferences).isNotEmpty();
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("firstks"))
                .collect(Collectors.toList())).hasSize(2);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("secondks"))
                .collect(Collectors.toList())).hasSize(1);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("thirdks"))
                .collect(Collectors.toList())).hasSize(1);
    }

    private void mockKeyspace(String keyspace, Set<String> tables)
    {
        CqlIdentifier keySpaceIdentifier = CqlIdentifier.fromInternal(keyspace);
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keySpaceIdentifier, k -> {
            KeyspaceMetadata mockedKeyspace = mock(KeyspaceMetadata.class);
            when(mockedKeyspace.getName()).thenReturn(keySpaceIdentifier);
            Map<CqlIdentifier, TableMetadata> tableMetadatas = new HashMap<>();
            for (String table : tables)
            {
                TableMetadata tableMetadata = mock(TableMetadata.class);
                when(tableMetadata.getId()).thenReturn(Optional.of(UUID.randomUUID()));
                when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(table));
                when(tableMetadata.getKeyspace()).thenReturn(keySpaceIdentifier);
                tableMetadatas.put(CqlIdentifier.fromInternal(table), tableMetadata);
                when(mockedKeyspace.getTable(eq(table))).thenReturn(Optional.of(tableMetadata));
            }
            when(mockedKeyspace.getTables()).thenReturn(tableMetadatas);
            return mockedKeyspace;
        });
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
    }

    @Test
    public void testNullWithoutKeyspace()
    {
        assertThat(tableReferenceFactory.forTable("keyspace", "table")).isNull();
    }

    @Test
    public void testNullWithoutTable()
    {
        mockEmptyKeyspace("keyspace");

        assertThat(tableReferenceFactory.forTable("keyspace", "table")).isNull();
    }

    private void mockEmptyKeyspace(String keyspace)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(keyspace));

        CqlIdentifier keySpaceIdentifier = CqlIdentifier.fromInternal(keyspace);
        mockedKeyspaces.put(keySpaceIdentifier, keyspaceMetadata);

        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
    }

    @Test
    public void testTableDoesNotExist()
    {
        TableMetadata tableMetadata = mockRemovedTable("keyspace", "table");

        assertThat(tableReferenceFactory.forTable("keyspace", "table")).isNull();

        TableReference tableReference = tableReferenceFactory.forTable(tableMetadata);
        assertThat(tableReference.getKeyspace()).isEqualTo(tableMetadata.getKeyspace().asInternal());
        assertThat(tableReference.getTable()).isEqualTo(tableMetadata.getName().asInternal());
        assertThat(tableReference.getId()).isEqualTo(tableMetadata.getId().get());
    }

    private TableMetadata mockRemovedTable(String keyspace, String table)
    {
        CqlIdentifier keySpaceIdentifier = CqlIdentifier.fromInternal(keyspace);
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keySpaceIdentifier, k -> {
            KeyspaceMetadata mockedKeyspace = mock(KeyspaceMetadata.class);
            when(mockedKeyspace.getName()).thenReturn(keySpaceIdentifier);
            return mockedKeyspace;
        });

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getId()).thenReturn(Optional.of(UUID.randomUUID()));
        when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(table));
        doReturn(keyspaceMetadata.getName()).when(tableMetadata).getKeyspace();

        when(keyspaceMetadata.getTable(eq(table))).thenReturn(Optional.empty());
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
        return tableMetadata;
    }

    private void mockTable(String keyspace, String table)
    {
        CqlIdentifier keySpaceIdentifier = CqlIdentifier.fromInternal(keyspace);
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keySpaceIdentifier, k -> {
            KeyspaceMetadata mockedKeyspace = mock(KeyspaceMetadata.class);
            when(mockedKeyspace.getName()).thenReturn(keySpaceIdentifier);
            return mockedKeyspace;
        });

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getId()).thenReturn(Optional.of(UUID.randomUUID()));
        when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(table));
        doReturn(keyspaceMetadata.getName()).when(tableMetadata).getKeyspace();

        when(keyspaceMetadata.getTable(eq(table))).thenReturn(Optional.of(tableMetadata));
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
    }
}
