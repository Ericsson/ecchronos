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
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * A table reference factory using tables existing in Cassandra.
 * Each unique table contains one specific table reference to avoid creating a lot of copies of table references.
 */
public class TableReferenceFactoryImpl implements TableReferenceFactory
{
    private final Metadata metadata;

    public TableReferenceFactoryImpl(Metadata metadata)
    {
        this.metadata = Preconditions.checkNotNull(metadata, "Metadata must be set");
    }

    @Override
    public TableReference forTable(String keyspace, String table)
    {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (keyspaceMetadata == null)
        {
            return null;
        }
        TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        if (tableMetadata == null)
        {
            return null;
        }

        return new UuidTableReference(tableMetadata);
    }

    @Override
    public TableReference forTable(TableMetadata table)
    {
        return new UuidTableReference(table);
    }

    @Override
    public Set<TableReference> forKeyspace(String keyspace) throws EcChronosException
    {
        Set<TableReference> tableReferences = new HashSet<>();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        if (keyspaceMetadata == null)
        {
            throw new EcChronosException("Keyspace " + keyspace + " does not exist");
        }
        for (TableMetadata table : keyspaceMetadata.getTables())
        {
            tableReferences.add(new UuidTableReference(table));
        }
        return tableReferences;
    }

    @Override
    public Set<TableReference> forCluster()
    {
        Set<TableReference> tableReferences = new HashSet<>();
        for (KeyspaceMetadata keyspace : metadata.getKeyspaces())
        {
            for (TableMetadata table : keyspace.getTables())
            {
                tableReferences.add(new UuidTableReference(table));
            }
        }
        return tableReferences;
    }

    class UuidTableReference implements TableReference
    {
        private final UUID uuid;
        private final String keyspace;
        private final String table;

        UuidTableReference(TableMetadata tableMetadata)
        {
            uuid = tableMetadata.getId();
            keyspace = tableMetadata.getKeyspace().getName();
            table = tableMetadata.getName();
        }

        @Override
        public UUID getId()
        {
            return uuid;
        }

        @Override
        public String getKeyspace()
        {
            return keyspace;
        }

        @Override
        public String getTable()
        {
            return table;
        }

        @Override
        public String toString()
        {
            return keyspace + "." + table;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UuidTableReference that = (UuidTableReference) o;
            return uuid.equals(that.uuid) && keyspace.equals(that.keyspace) && table.equals(that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(uuid, keyspace, table);
        }
    }
}
