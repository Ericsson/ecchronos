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
package com.ericsson.bss.cassandra.ecchronos.core.metadata;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.util.Strings;

import java.util.Optional;

/**
 * Helper class to retrieve keyspace and table metadata,
 * this should be preferred than doing session.getMetadata().getKeyspace(name) or keyspaceMetadata.getTable(name)
 * Main purpose is to not have to care if the keyspace/table string representation is quoted or not.
 * In driver, keyspaces/tables with camelCase needs to be quoted.
 */
public final class Metadata
{
    private Metadata()
    {
        //Intentionally left empty
    }

    /**
     * Retrieves the metadata for a keyspace, quoting the name if needed.
     *
     * @param session the CQL session to retrieve metadata from.
     * @param keyspace the keyspace name.
     * @return an Optional containing the keyspace metadata, or empty if not found.
     */
    public static Optional<KeyspaceMetadata> getKeyspace(final CqlSession session, final String keyspace)
    {
        String keyspaceName = quoteIfNeeded(keyspace);
        return session.getMetadata().getKeyspace(keyspaceName);
    }

    /**
     * Retrieves the metadata for a table from the given keyspace, quoting the name if needed.
     *
     * @param keyspaceMetadata the keyspace metadata to search within.
     * @param table the table name.
     * @return an Optional containing the table metadata, or empty if not found.
     */
    public static Optional<TableMetadata> getTable(final KeyspaceMetadata keyspaceMetadata, final String table)
    {
        String tableName = quoteIfNeeded(table);
        return keyspaceMetadata.getTable(tableName);
    }

    /**
     * Quotes a keyspace or table name with double quotes if needed for CQL compatibility.
     *
     * @param keyspaceOrTable the keyspace or table name to conditionally quote.
     * @return the quoted name if quoting is needed, or the original name otherwise.
     */
    public static String quoteIfNeeded(final String keyspaceOrTable)
    {
        return Strings.needsDoubleQuotes(keyspaceOrTable) && !Strings.isDoubleQuoted(keyspaceOrTable)
                ? Strings.doubleQuote(keyspaceOrTable)
                : keyspaceOrTable;
    }
}

