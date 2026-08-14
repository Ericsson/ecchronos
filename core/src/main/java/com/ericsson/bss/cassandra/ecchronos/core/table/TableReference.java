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
package com.ericsson.bss.cassandra.ecchronos.core.table;

import java.util.UUID;

/**
 * An interface containing keyspace/table mapping to avoid passing around two strings to refer to one specific table.
 */
public interface TableReference
{
    /**
     * Gets the unique identifier of this table.
     *
     * @return the table UUID.
     */
    UUID getId();

    /**
     * Gets the table name.
     *
     * @return the table name.
     */
    String getTable();

    /**
     * Gets the keyspace name this table belongs to.
     *
     * @return the keyspace name.
     */
    String getKeyspace();

    /**
     * Gets the gc_grace_seconds setting for this table.
     *
     * @return the gc_grace_seconds value.
     */
    int getGcGraceSeconds();

    /**
     * Checks whether this table uses TimeWindowCompactionStrategy (TWCS).
     *
     * @return true if the table uses TWCS, false otherwise.
     */
    boolean getTwcs();
}
