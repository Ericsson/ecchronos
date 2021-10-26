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

import com.datastax.driver.core.TableMetadata;

/**
 * A factory that generates table references based on keyspace and table name.
 */
public interface TableReferenceFactory
{
    /**
     * Get a table reference for the provided keyspace/table pair.
     *
     * @param keyspace The keyspace name.
     * @param table The table name.
     * @return A table reference for the provided keyspace/table pair or null if table does not exist.
     */
    TableReference forTable(String keyspace, String table);

    /**
     * Get a table reference for the provided TableMetadata.
     *
     * @param table the TableMetadata.
     * @return A table reference for the provided keyspace/table pair.
     */
    TableReference forTable(TableMetadata table);
}
