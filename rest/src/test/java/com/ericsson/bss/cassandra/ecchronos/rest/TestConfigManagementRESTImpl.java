/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestConfigManagementRESTImpl
{
    @Mock
    private ScheduleManager myScheduleManager;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    private ConfigManagementRESTImpl controller;

    @Before
    public void setup()
    {
        controller = new ConfigManagementRESTImpl(myScheduleManager, myJmxProxyFactory);
        when(myScheduleManager.getSessionWindowInMs()).thenReturn(300000L);
        when(myScheduleManager.getCooldownInMs()).thenReturn(0L);
        when(myScheduleManager.getLocksPerResource()).thenReturn(3);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
    }

    @Test
    public void testGetConfig()
    {
        ResponseEntity<Map<String, Object>> response = controller.getConfig();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        Map<String, Object> body = response.getBody();
        assertThat(body.get("session_window_ms")).isEqualTo(300000L);
        assertThat(body.get("cooldown_ms")).isEqualTo(0L);
        assertThat(body.get("locks_per_resource")).isEqualTo(3);
        assertThat(body.get("max_wait_time_minutes")).isEqualTo(40);
    }

    @Test
    public void testPatchSessionWindow()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("session_window_ms", 600000L);

        controller.patchConfig(patch);

        verify(myScheduleManager).setSessionWindowInMs(600000L);
    }

    @Test
    public void testPatchCooldown()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("cooldown_ms", 30000L);

        controller.patchConfig(patch);

        verify(myScheduleManager).setCooldownInMs(30000L);
    }

    @Test
    public void testPatchLocksPerResource()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("locks_per_resource", 5);

        controller.patchConfig(patch);

        verify(myScheduleManager).setLocksPerResource(5);
    }

    @Test
    public void testPatchMultipleFields()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("session_window_ms", 120000L);
        patch.put("cooldown_ms", 5000L);

        controller.patchConfig(patch);

        verify(myScheduleManager).setSessionWindowInMs(120000L);
        verify(myScheduleManager).setCooldownInMs(5000L);
    }

    @Test
    public void testPatchEmptyBodyChangesNothing()
    {
        Map<String, Object> patch = new HashMap<>();

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    @Test
    public void testPatchInvalidSessionWindowReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("session_window_ms", -1L);

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("session_window must be > 0");
    }

    @Test
    public void testPatchInvalidLocksPerResourceReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("locks_per_resource", 0);

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("locks_per_resource must be >= 1");
    }

    @Test
    public void testPatchMaxWaitTime()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("max_wait_time_minutes", 60);

        controller.patchConfig(patch);

        verify(myJmxProxyFactory).setMaxWaitTimeInMinutes(60);
    }

    @Test
    public void testPatchInvalidMaxWaitTimeReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("max_wait_time_minutes", 0);

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("max_wait_time_minutes must be > 0");
        verify(myJmxProxyFactory, never()).setMaxWaitTimeInMinutes(anyInt());
    }
}
