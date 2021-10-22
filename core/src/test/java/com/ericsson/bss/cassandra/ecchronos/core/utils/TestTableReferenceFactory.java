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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestTableReferenceFactory
{
    @Mock
    private Metadata mockMetadata;

    private TableReferenceFactory tableReferenceFactory;

    private Map<String, KeyspaceMetadata> mockedKeyspaces = new HashMap<>();

    @Before
    public void setup()
    {
        tableReferenceFactory = new TableReferenceFactoryImpl(mockMetadata);
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
        when(keyspaceMetadata.getName()).thenReturn(keyspace);

        mockedKeyspaces.put(keyspace, keyspaceMetadata);

        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(keyspaceMetadata);
    }

    @Test
    public void testTableDoesNotExist()
    {
        TableMetadata tableMetadata = mockRemovedTable("keyspace", "table");

        assertThat(tableReferenceFactory.forTable("keyspace", "table")).isNull();

        TableReference tableReference = tableReferenceFactory.forTable(tableMetadata);
        assertThat(tableReference.getKeyspace()).isEqualTo(tableMetadata.getKeyspace().getName());
        assertThat(tableReference.getTable()).isEqualTo(tableMetadata.getName());
        assertThat(tableReference.getId()).isEqualTo(tableMetadata.getId());
    }

    private TableMetadata mockRemovedTable(String keyspace, String table)
    {
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keyspace, k -> {
            KeyspaceMetadata mockedKeyspace = mock(KeyspaceMetadata.class);
            when(mockedKeyspace.getName()).thenReturn(keyspace);
            return mockedKeyspace;
        });

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getId()).thenReturn(UUID.randomUUID());
        when(tableMetadata.getName()).thenReturn(table);
        when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);

        when(keyspaceMetadata.getTable(eq(table))).thenReturn(null);
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(keyspaceMetadata);
        return tableMetadata;
    }

    private void mockTable(String keyspace, String table)
    {
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keyspace, k -> {
            KeyspaceMetadata mockedKeyspace = mock(KeyspaceMetadata.class);
            when(mockedKeyspace.getName()).thenReturn(keyspace);
            return mockedKeyspace;
        });

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getId()).thenReturn(UUID.randomUUID());
        when(tableMetadata.getName()).thenReturn(table);
        when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);

        when(keyspaceMetadata.getTable(eq(table))).thenReturn(tableMetadata);
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(keyspaceMetadata);
    }
}
