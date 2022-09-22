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

    public static Optional<KeyspaceMetadata> getKeyspace(final CqlSession session, final String keyspace)
    {
        String keyspaceName = quoteIfNeeded(keyspace);
        return session.getMetadata().getKeyspace(keyspaceName);
    }

    public static Optional<TableMetadata> getTable(final KeyspaceMetadata keyspaceMetadata, final String table)
    {
        String tableName = quoteIfNeeded(table);
        return keyspaceMetadata.getTable(tableName);
    }

    public static String quoteIfNeeded(final String keyspaceOrTable)
    {
        return Strings.needsDoubleQuotes(keyspaceOrTable) && !Strings.isDoubleQuoted(keyspaceOrTable)
                ? Strings.doubleQuote(keyspaceOrTable)
                : keyspaceOrTable;
    }
}
