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
package com.ericsson.bss.cassandra.ecchronos.core.state;

import org.junit.After;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestApplicationStateHolder
{
    @After
    public void cleanup()
    {
        ApplicationStateHolder.getInstance().clear();
    }

    @Test
    public void testGetInstance()
    {
        ApplicationState instance1 = ApplicationStateHolder.getInstance();
        ApplicationState instance2 = ApplicationStateHolder.getInstance();
        
        assertThat(instance1).isSameAs(instance2);
        assertThat(instance1).isNotNull();
    }

    @Test
    public void testInitialState()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        Optional<Object> running = state.get("running");
        assertThat(running).isPresent();
        assertThat(running.get()).isEqualTo(true);
    }

    @Test
    public void testPutAndGet()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("test.key", "test.value");
        
        Optional<Object> result = state.get("test.key");
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("test.value");
    }

    @Test
    public void testPutAndGetWithType()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("string.key", "string.value");
        state.put("integer.key", 42);
        state.put("boolean.key", true);
        
        Optional<String> stringResult = state.get("string.key", String.class);
        Optional<Integer> intResult = state.get("integer.key", Integer.class);
        Optional<Boolean> boolResult = state.get("boolean.key", Boolean.class);
        
        assertThat(stringResult).isPresent();
        assertThat(stringResult.get()).isEqualTo("string.value");
        assertThat(intResult).isPresent();
        assertThat(intResult.get()).isEqualTo(42);
        assertThat(boolResult).isPresent();
        assertThat(boolResult.get()).isEqualTo(true);
    }

    @Test
    public void testGetNonExistentKey()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        Optional<Object> result = state.get("non.existent.key");
        assertThat(result).isEmpty();
    }

    @Test
    public void testGetWithWrongType()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("string.key", "string.value");
        
        Optional<Integer> result = state.get("string.key", Integer.class);
        assertThat(result).isEmpty();
    }

    @Test
    public void testRemove()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("remove.key", "remove.value");
        assertThat(state.get("remove.key")).isPresent();
        
        Optional<Object> removed = state.remove("remove.key");
        assertThat(removed).isPresent();
        assertThat(removed.get()).isEqualTo("remove.value");
        assertThat(state.get("remove.key")).isEmpty();
    }

    @Test
    public void testRemoveNonExistentKey()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        Optional<Object> removed = state.remove("non.existent.key");
        assertThat(removed).isEmpty();
    }

    @Test
    public void testGetAll()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("key1", "value1");
        state.put("key2", 42);
        
        Map<String, Object> all = state.getAll();
        assertThat(all).containsEntry("running", true);
        assertThat(all).containsEntry("key1", "value1");
        assertThat(all).containsEntry("key2", 42);
    }

    @Test
    public void testClear()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("key1", "value1");
        state.put("key2", "value2");
        assertThat(state.getAll()).hasSize(4); // "key1", "key2", "running" and "timestamp"
        
        state.clear();

        assertThat(state.getAll()).hasSize(2); // "running" and "timestamp"
        assertThat(state.getAll()).containsEntry("running", true);
    }

    @Test
    public void testNestedKeys()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("level1.level2.key", "nested.value");
        
        Optional<Object> result = state.get("level1.level2.key");
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("nested.value");
    }

    @Test
    public void testPutNullValue()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("null.key", "initial.value");
        assertThat(state.get("null.key")).isPresent();
        
        state.put("null.key", null);
        assertThat(state.get("null.key")).isEmpty();
    }

    @Test
    public void testRunningKeyOnlyAcceptsBooleans()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        state.put("running", false);
        assertThat(state.get("running")).contains(false);
        
        assertThatThrownBy(() -> state.put("running", "string"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("running key can only be set to boolean values");
            
        assertThatThrownBy(() -> state.put("running", 42))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRunningKeyCannotBeRemoved()
    {
        ApplicationState state = ApplicationStateHolder.getInstance();
        
        assertThatThrownBy(() -> state.remove("running"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("running key cannot be removed");
    }

}
