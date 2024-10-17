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

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Set;

/**
 * Interface for retrieving tables replicated by the local node.
 * The purpose of this interface is to abstract away java-driver related mocking from other components
 * trying to retrieve the tables that should be repaired.
 */
public interface ReplicatedTableProvider
{
    /**
     * @return The full set of tables replicated on the local node which should be repaired.
     */
    Set<TableReference> getAll();

    /**
     * Check if a keyspace should be repaired.
     *
     * @param keyspace The keyspace to check.
     * @return True if the provided keyspace should be repaired.
     */
    boolean accept(Node node, String keyspace);
}
