/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.state;

import java.util.Map;
import java.util.Optional;

/**
 * Interface for managing application state throughout the ecChronos application.
 */
public interface ApplicationState
{
    /**
     * Store a value in the application state.
     *
     * @param key The key to store the value under
     * @param value The value to store
     */
    void put(String key, Object value);

    /**
     * Retrieve a value from the application state.
     *
     * @param key The key to retrieve
     * @return Optional containing the value if present
     */
    Optional<Object> get(String key);

    /**
     * Retrieve a value from the application state with a specific type.
     *
     * @param key The key to retrieve
     * @param type The expected type of the value
     * @return Optional containing the typed value if present and of correct type
     */
    <T> Optional<T> get(String key, Class<T> type);

    /**
     * Remove a value from the application state.
     *
     * @param key The key to remove
     * @return Optional containing the removed value if it existed
     */
    Optional<Object> remove(String key);

    /**
     * Get all state entries.
     *
     * @return Map of all key-value pairs in the state
     */
    Map<String, Object> getAll();

    /**
     * Clear all state entries.
     */
    void clear();

}
