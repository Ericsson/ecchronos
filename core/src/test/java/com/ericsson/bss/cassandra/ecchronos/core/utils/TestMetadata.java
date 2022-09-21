/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestMetadata
{
    @Mock
    CqlSession cqlSessionMock;

    @Mock
    com.datastax.oss.driver.api.core.metadata.Metadata metadataMock;

    @Mock
    KeyspaceMetadata keyspaceMetadataMock;

    @Mock
    TableMetadata tableMetadataMock;

    @Before
    public void setup()
    {
        when(cqlSessionMock.getMetadata()).thenReturn(metadataMock);
    }

    @Test
    public void testGetKeyspace()
    {
        String keyspace = "keyspace1";
        when(metadataMock.getKeyspace(eq(keyspace))).thenReturn(Optional.of(keyspaceMetadataMock));
        assertThat(Metadata.getKeyspace(cqlSessionMock, keyspace)).isNotEmpty();
    }

    @Test
    public void testGetKeyspaceWithCamelCase()
    {
        String keyspace = "keyspace1WithCamelCase";
        when(metadataMock.getKeyspace(eq("\""+keyspace+"\""))).thenReturn(Optional.of(keyspaceMetadataMock));
        assertThat(Metadata.getKeyspace(cqlSessionMock, keyspace)).isNotEmpty();
    }

    @Test
    public void testGetKeyspaceWithCamelCaseAlreadyQuoted()
    {
        String keyspace = "keyspace1WithCamelCase";
        when(metadataMock.getKeyspace(eq("\""+keyspace+"\""))).thenReturn(Optional.of(keyspaceMetadataMock));
        assertThat(Metadata.getKeyspace(cqlSessionMock, "\""+keyspace+"\"")).isNotEmpty();
    }

    @Test
    public void testGetTable()
    {
        String table = "table1";
        when(keyspaceMetadataMock.getTable(eq(table))).thenReturn(Optional.of(tableMetadataMock));
        assertThat(Metadata.getTable(keyspaceMetadataMock, table)).isNotEmpty();
    }

    @Test
    public void testGetTableWithCamelCase()
    {
        String table = "table1WithCamelCase";
        when(keyspaceMetadataMock.getTable(eq("\""+table+"\""))).thenReturn(Optional.of(tableMetadataMock));
        assertThat(Metadata.getTable(keyspaceMetadataMock, table)).isNotEmpty();
    }

    @Test
    public void testGetTableWithCamelCaseAlreadyQuoted()
    {
        String table = "table1WithCamelCase";
        when(keyspaceMetadataMock.getTable(eq("\""+table+"\""))).thenReturn(Optional.of(tableMetadataMock));
        assertThat(Metadata.getTable(keyspaceMetadataMock, "\""+table+"\"")).isNotEmpty();
    }
}
