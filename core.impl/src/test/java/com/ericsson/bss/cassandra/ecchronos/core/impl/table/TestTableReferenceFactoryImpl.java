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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestTableReferenceFactoryImpl
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
        mockTable("keyspace1", "table1");

        TableReference tableReference = tableReferenceFactory.forTable("keyspace1", "table1");

        assertThat(tableReference.getKeyspace()).isEqualTo("keyspace1");
        assertThat(tableReference.getTable()).isEqualTo("table1");
    }

    @Test
    public void testGetKeyspaceAndTableNameWithCamelCase()
    {
        mockTable("keyspaceWithCamelCase1", "tableWithCamelCase1");

        TableReference tableReference = tableReferenceFactory.forTable("keyspaceWithCamelCase1", "tableWithCamelCase1");

        assertThat(tableReference.getKeyspace()).isEqualTo("keyspaceWithCamelCase1");
        assertThat(tableReference.getTable()).isEqualTo("tableWithCamelCase1");
    }

    @Test
    public void testNewTableIsNotEqual()
    {
        mockTable("keyspace1", "table1");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace1", "table1");

        mockTable("keyspace1", "table1"); // New table uuid
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace1", "table1");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
    }

    @Test
    public void testSameTableIsEqual()
    {
        mockTable("keyspace1", "table1");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace1", "table1");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace1", "table1");

        assertThat(tableReference1).isEqualTo(tableReference2);
    }

    @Test
    public void testDifferentKeyspacesNotEqual()
    {
        mockTable("keyspace1", "table1");
        mockTable("keyspace2", "table1");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace1", "table1");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace2", "table1");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isNotEqualTo(tableReference2.hashCode());
    }

    @Test
    public void testDifferentTablesNotEqual()
    {
        mockTable("keyspace1", "table1");
        mockTable("keyspace1", "table2");

        TableReference tableReference1 = tableReferenceFactory.forTable("keyspace1", "table1");
        TableReference tableReference2 = tableReferenceFactory.forTable("keyspace1", "table2");

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
    public void testForKeyspaceKeyspaceWithCamelCase() throws EcChronosException
    {
        Set<String> tables = new HashSet<>();
        tables.add("table1");
        tables.add("table2");
        mockKeyspace("keyspaceWithCamelCase111", tables);
        mockKeyspace("keyspace222", Collections.singleton("table1"));
        Set<TableReference> tableReferences = tableReferenceFactory.forKeyspace("keyspaceWithCamelCase111");
        assertThat(tableReferences).hasSize(2);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("keyspaceWithCamelCase111"))
                .collect(Collectors.toList())).isNotEmpty();
    }

    @Test
    public void testForKeyspaceNoTables() throws EcChronosException
    {
        mockEmptyKeyspace("keyspace_empty");
        Set<TableReference> tableReferences = tableReferenceFactory.forKeyspace("keyspace_empty");
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

    @Test
    public void testForClusterWithCamelCase()
    {
        Set<String> firstKsTables = new HashSet<>();
        firstKsTables.add("table1");
        firstKsTables.add("tablEE2");
        mockKeyspace("firstKs", firstKsTables);
        mockKeyspace("secondks", Collections.singleton("table1"));
        mockKeyspace("thirdKs", Collections.singleton("table1"));

        when(mockMetadata.getKeyspaces()).thenReturn(mockedKeyspaces);
        Set<TableReference> tableReferences = tableReferenceFactory.forCluster();
        assertThat(tableReferences).isNotEmpty();
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("firstKs"))
                .collect(Collectors.toList())).hasSize(2);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("secondks"))
                .collect(Collectors.toList())).hasSize(1);
        assertThat(tableReferences.stream()
                .filter(t -> t.getKeyspace().equals("thirdKs"))
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
                Map<CqlIdentifier, Object> options = new HashMap<>();
                options.put(CqlIdentifier.fromInternal("gc_grace_seconds"), MockTableReferenceFactory.DEFAULT_GC_GRACE_SECONDS);
                when(tableMetadata.getOptions()).thenReturn(options);
                tableMetadatas.put(CqlIdentifier.fromInternal(table), tableMetadata);
                when(mockedKeyspace.getTable(eq(table))).thenReturn(Optional.of(tableMetadata));
            }
            when(mockedKeyspace.getTables()).thenReturn(tableMetadatas);
            return mockedKeyspace;
        });
        if (Strings.needsDoubleQuotes(keyspace))
        {
            when(mockMetadata.getKeyspace(eq("\""+keyspace+"\""))).thenReturn(Optional.of(keyspaceMetadata));
        }
        else
        {
            when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
        }
    }

    @Test
    public void testNullWithoutKeyspace()
    {
        assertThat(tableReferenceFactory.forTable("keyspace1", "table1")).isNull();
    }

    @Test
    public void testNullWithoutTable()
    {
        mockEmptyKeyspace("keyspace1");

        assertThat(tableReferenceFactory.forTable("keyspace1", "table1")).isNull();
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
        TableMetadata tableMetadata = mockRemovedTable("keyspace1", "table1");

        assertThat(tableReferenceFactory.forTable("keyspace1", "table1")).isNull();

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
        Map<CqlIdentifier, Object> options = new HashMap<>();
        options.put(CqlIdentifier.fromInternal("gc_grace_seconds"), MockTableReferenceFactory.DEFAULT_GC_GRACE_SECONDS);
        when(tableMetadata.getOptions()).thenReturn(options);
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
        Map<CqlIdentifier, Object> options = new HashMap<>();
        options.put(CqlIdentifier.fromInternal("gc_grace_seconds"), MockTableReferenceFactory.DEFAULT_GC_GRACE_SECONDS);
        when(tableMetadata.getOptions()).thenReturn(options);
        doReturn(keyspaceMetadata.getName()).when(tableMetadata).getKeyspace();

        if (Strings.needsDoubleQuotes(table))
        {
            when(keyspaceMetadata.getTable(eq("\""+table+"\""))).thenReturn(Optional.of(tableMetadata));
        }
        else
        {
            when(keyspaceMetadata.getTable(eq(table))).thenReturn(Optional.of(tableMetadata));
        }
        if (Strings.needsDoubleQuotes(keyspace))
        {
            when(mockMetadata.getKeyspace(eq("\""+keyspace+"\""))).thenReturn(Optional.of(keyspaceMetadata));
        }
        else
        {
            when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadata));
        }
    }
}
