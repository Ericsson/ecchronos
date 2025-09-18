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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.ericsson.bss.cassandra.ecchronos.core.state.ApplicationStateHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStateRepairManagementRESTImpl
{
    private StateRepairManagementRESTImpl stateREST;

    @Before
    public void setup()
    {
        stateREST = new StateRepairManagementRESTImpl();
        ApplicationStateHolder.getInstance().clear();
    }

    @After
    public void cleanup()
    {
        ApplicationStateHolder.getInstance().clear();
    }

    @Test
    public void testGetAllStateEmpty()
    {
        ResponseEntity<Map<String, Object>> response = stateREST.getAllState();
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(2);
        assertThat(response.getBody()).containsEntry("running", true);
        assertThat(response.getBody()).containsKey("timestamp");
    }

    @Test
    public void testGetAllStateWithValues()
    {
        ApplicationStateHolder.getInstance().put("key1", "value1");
        ApplicationStateHolder.getInstance().put("key2", 42);
        
        ResponseEntity<Map<String, Object>> response = stateREST.getAllState();
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(4); // ... plus "running" and "timestamp"
        assertThat(response.getBody()).containsEntry("key1", "value1");
        assertThat(response.getBody()).containsEntry("key2", 42);
        assertThat(response.getBody()).containsEntry("running", true);
        assertThat(response.getBody()).containsKey("timestamp");
    }

    @Test
    public void testGetStateExistingKey()
    {
        ApplicationStateHolder.getInstance().put("test.key", "test.value");
        
        ResponseEntity<Object> response = stateREST.getState("test.key");
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("test.value");
    }

    @Test
    public void testGetStateNonExistentKey()
    {
        ResponseEntity<Object> response = stateREST.getState("non.existent.key");
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).isNull();
    }

}
