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
 * Interface for retrieving storage usage for all tables this nodes should repair.
 */
public interface TableStorageStates
{
    /**
     * Get the data size of the provided table on the local node.
     *
     * @param tableReference The table to get the data size of.
     * @return The data size of the provided table on this node.
     */
    long getDataSize(UUID nodeID, TableReference tableReference);

    /**
     * Get the total data size of all tables on the local node.
     *
     * @return The data size of all tables on this node.
     */
    long getDataSize(UUID nodeID);
}
