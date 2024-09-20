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

import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public class MockTableReferenceFactory implements TableReferenceFactory
{
    public static final int DEFAULT_GC_GRACE_SECONDS = 7200;
    private static final ConcurrentMap<TableKey, TableReference> tableReferences = new ConcurrentHashMap<>();

    @Override
    public TableReference forTable(String keyspace, String table)
    {
        return tableReference(keyspace, table);
    }

    @Override
    public TableReference forTable(TableMetadata table)
    {
        return tableReference(table);
    }

    @Override
    public Set<TableReference> forKeyspace(String keyspace) throws EcChronosException
    {
        Set<TableReference> tableReferences = new HashSet<>();
        tableReferences.add(tableReference(keyspace, "table1"));
        tableReferences.add(tableReference(keyspace, "table2"));
        tableReferences.add(tableReference(keyspace, "table3"));
        return tableReferences;
    }

    @Override
    public Set<TableReference> forCluster()
    {
        Set<TableReference> tableReferences = new HashSet<>();
        tableReferences.add(tableReference("keyspace1", "table1"));
        tableReferences.add(tableReference("keyspace1", "table2"));
        tableReferences.add(tableReference("keyspace1", "table3"));
        tableReferences.add(tableReference("keyspace2", "table4"));
        tableReferences.add(tableReference("keyspace3", "table5"));
        return tableReferences;
    }

    public static TableReference tableReference(String keyspace, String table)
    {
        return tableReference(keyspace, table, DEFAULT_GC_GRACE_SECONDS);
    }

    public static TableReference tableReference(String keyspace, String table, int gcGraceSeconds)
    {
        TableKey tableKey = new TableKey(keyspace, table);
        TableReference tableReference = tableReferences.get(tableKey);
        if (tableReference == null)
        {
            tableReference = tableReferences.computeIfAbsent(tableKey,
                    tb -> new MockTableReference(UUID.randomUUID(), keyspace, table, gcGraceSeconds));
        }

        return tableReference;
    }

    public static TableReference tableReference(TableMetadata table)
    {
        return new MockTableReference(table);
    }

    static class MockTableReference implements TableReference
    {
        private final UUID id;
        private final String keyspace;
        private final String table;
        private final int gcGraceSeconds;

        MockTableReference(UUID id, String keyspace, String table)
        {
            this(id, keyspace, table, DEFAULT_GC_GRACE_SECONDS);
        }

        MockTableReference(TableMetadata tableMetadata)
        {
            this(tableMetadata.getId().get(), tableMetadata.getKeyspace().asInternal(),
                    tableMetadata.getName().asInternal(),
                    (int) tableMetadata.getOptions().get(CqlIdentifier.fromInternal("gc_grace_seconds")));
        }

        MockTableReference(UUID id, String keyspace, String table, int gcGraceSeconds)
        {
            this.id = id;
            this.keyspace = keyspace;
            this.table = table;
            this.gcGraceSeconds = gcGraceSeconds;
        }

        @Override
        public UUID getId()
        {
            return id;
        }

        @Override
        public String getTable()
        {
            return table;
        }

        @Override
        public String getKeyspace()
        {
            return keyspace;
        }

        @Override
        public int getGcGraceSeconds()
        {
            return gcGraceSeconds;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MockTableReference that = (MockTableReference) o;
            return id.equals(that.id) &&
                    keyspace.equals(that.keyspace) &&
                    table.equals(that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, keyspace, table);
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s (mock)", keyspace, table);
        }
    }

    static class TableKey
    {
        private final String keyspace;
        private final String table;

        TableKey(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TableKey tableKey = (TableKey) o;
            return keyspace.equals(tableKey.keyspace) &&
                    table.equals(tableKey.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table);
        }
    }
}

