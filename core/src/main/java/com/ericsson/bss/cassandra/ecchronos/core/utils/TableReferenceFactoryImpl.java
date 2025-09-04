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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * A table reference factory using tables existing in Cassandra.
 * Each unique table contains one specific table reference to avoid creating a lot of copies of table references.
 */
public class TableReferenceFactoryImpl implements TableReferenceFactory
{
    private final CqlSession session;

    public TableReferenceFactoryImpl(final CqlSession aSession)
    {
        this.session = Preconditions.checkNotNull(aSession, "Session must be set");
    }

    @Override
    public final TableReference forTable(final String keyspace, final String table)
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = Metadata.getKeyspace(session, keyspace);
        if (!keyspaceMetadata.isPresent())
        {
            return null;
        }
        Optional<TableMetadata> tableMetadata = Metadata.getTable(keyspaceMetadata.get(), table);
        if (!tableMetadata.isPresent())
        {
            return null;
        }

        return new UuidTableReference(tableMetadata.get());
    }

    @Override
    public final TableReference forTable(final TableMetadata table)
    {
        return new UuidTableReference(table);
    }

    @Override
    public final Set<TableReference> forKeyspace(final String keyspace) throws EcChronosException
    {
        Set<TableReference> tableReferences = new HashSet<>();
        Optional<KeyspaceMetadata> keyspaceMetadata = Metadata.getKeyspace(session, keyspace);
        if (!keyspaceMetadata.isPresent())
        {
            throw new EcChronosException("Keyspace " + keyspace + " does not exist");
        }
        for (TableMetadata table : keyspaceMetadata.get().getTables().values())
        {
            tableReferences.add(new UuidTableReference(table));
        }
        return tableReferences;
    }

    @Override
    public final Set<TableReference> forCluster()
    {
        Set<TableReference> tableReferences = new HashSet<>();
        for (KeyspaceMetadata keyspace : session.getMetadata().getKeyspaces().values())
        {
            for (TableMetadata table : keyspace.getTables().values())
            {
                tableReferences.add(new UuidTableReference(table));
            }
        }
        return tableReferences;
    }

    record UuidTableReference(UUID uuid, String keyspace, String table, int gcGraceSeconds) implements TableReference
    {
        UuidTableReference(final TableMetadata tableMetadata)
        {
            this(tableMetadata.getId().get(),
                    tableMetadata.getKeyspace().asInternal(),
                    tableMetadata.getName().asInternal(),
                    (int) tableMetadata.getOptions().get(CqlIdentifier.fromInternal("gc_grace_seconds")));
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
        public int getGcGraceSeconds()
        {
            return gcGraceSeconds;
        }

        @Override
        public String toString()
        {
            return keyspace + "." + table;
        }
    }
}
