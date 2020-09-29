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
package com.ericsson.bss.cassandra.ecchronos.core;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockTableReferenceFactory implements TableReferenceFactory
{
    private static final ConcurrentMap<TableKey, TableReference> tableReferences = new ConcurrentHashMap<>();

    @Override
    public TableReference forTable(String keyspace, String table)
    {
        return tableReference(keyspace, table);
    }

    public static TableReference tableReference(String keyspace, String table)
    {
        TableKey tableKey = new TableKey(keyspace, table);
        TableReference tableReference = tableReferences.get(tableKey);
        if (tableReference == null)
        {
            tableReference = tableReferences.computeIfAbsent(tableKey, tb -> new MockTableReference(UUID.randomUUID(), keyspace, table));
        }

        return tableReference;
    }

    static class MockTableReference implements TableReference
    {
        private final UUID id;
        private final String keyspace;
        private final String table;

        MockTableReference(UUID id, String keyspace, String table)
        {
            this.id = id;
            this.keyspace = keyspace;
            this.table = table;
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
