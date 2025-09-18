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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe implementation of ApplicationState using ConcurrentHashMap.
 */
public final class ApplicationStateImpl implements ApplicationState
{
    private static final String KEY_RUNNING = "running";
    private static final String KEY_TIMESTAMP = "timestamp";

    private final Map<String, Object> state = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public void put(final String key, final Object value)
    {
        if (KEY_RUNNING.equals(key) && !(value instanceof Boolean))
        {
            throw new IllegalArgumentException("running key can only be set to boolean values");
        }

        if (value == null)
        {
            remove(key);
            return;
        }

        String[] parts = key.split("\\.");
        Map<String, Object> current = state;

        for (int i = 0; i < parts.length - 1; i++)
        {
            Object next = current.get(parts[i]);
            if (!(next instanceof Map))
            {
                next = new ConcurrentHashMap<String, Object>();
                current.put(parts[i], next);
            }
            current = (Map<String, Object>) next;
        }

        current.put(parts[parts.length - 1], value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Object> get(final String key)
    {
        String[] parts = key.split("\\.");
        Map<String, Object> current = state;

        for (int i = 0; i < parts.length - 1; i++)
        {
            Object next = current.get(parts[i]);
            if (!(next instanceof Map))
            {
                return Optional.empty();
            }
            current = (Map<String, Object>) next;
        }

        setTimeStamp();

        return Optional.ofNullable(current.get(parts[parts.length - 1]));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(final String key, final Class<T> type)
    {
        Optional<Object> value = get(key);
        if (value.isPresent() && type.isInstance(value.get()))
        {
            return Optional.of((T) value.get());
        }

        setTimeStamp();

        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Object> remove(final String key)
    {
        if (KEY_RUNNING.equals(key))
        {
            throw new IllegalArgumentException("running key cannot be removed");
        }

        String[] parts = key.split("\\.");
        Map<String, Object> current = state;

        for (int i = 0; i < parts.length - 1; i++)
        {
            Object next = current.get(parts[i]);
            if (!(next instanceof Map))
            {
                return Optional.empty();
            }
            current = (Map<String, Object>) next;
        }

        return Optional.ofNullable(current.remove(parts[parts.length - 1]));
    }

    @Override
    public Map<String, Object> getAll()
    {
        setTimeStamp();
        return Map.copyOf(state);
    }

    @Override
    public void clear()
    {
        state.clear();
        state.put(KEY_RUNNING, true);
    }

    private void setTimeStamp()
    {
        this.put(KEY_TIMESTAMP, System.currentTimeMillis());
    }

}
