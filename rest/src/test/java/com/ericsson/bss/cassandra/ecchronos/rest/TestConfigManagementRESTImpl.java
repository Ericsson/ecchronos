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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.HungRepairSessionRecovery;
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

    @Mock
    private HungRepairSessionRecovery myHungRepairSessionRecovery;

    private ConfigManagementRESTImpl controller;

    @Before
    public void setup()
    {
        controller = new ConfigManagementRESTImpl(
                myScheduleManager, myJmxProxyFactory, myHungRepairSessionRecovery);
        when(myScheduleManager.getSessionWindowInMs()).thenReturn(300000L);
        when(myScheduleManager.getCooldownInMs()).thenReturn(0L);
        when(myScheduleManager.getLocksPerResource()).thenReturn(3);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myHungRepairSessionRecovery.isEnabled()).thenReturn(false);
        when(myHungRepairSessionRecovery.getStallThresholdMs()).thenReturn(1800000L);
        when(myHungRepairSessionRecovery.isBypassCoordinatorCheck()).thenReturn(false);
        when(myHungRepairSessionRecovery.isForce()).thenReturn(false);
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
        assertThat(body.get("hung_repair_recovery_enabled")).isEqualTo(false);
        assertThat(body.get("hung_repair_stall_threshold_ms")).isEqualTo(1800000L);
        assertThat(body.get("hung_repair_bypass_coordinator_check")).isEqualTo(false);
        assertThat(body.get("hung_repair_force")).isEqualTo(false);
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

    @Test
    public void testPatchHungRepairRecoveryEnabled()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_recovery_enabled", true);

        controller.patchConfig(patch);

        verify(myHungRepairSessionRecovery).setEnabled(true);
    }

    @Test
    public void testPatchHungRepairStallThreshold()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_stall_threshold_ms", 900000L);

        controller.patchConfig(patch);

        verify(myHungRepairSessionRecovery).setStallThresholdMs(900000L);
    }

    @Test
    public void testPatchInvalidHungRepairThresholdReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_stall_threshold_ms", 0L);

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("hung_repair_stall_threshold_ms must be > 0");
        verify(myHungRepairSessionRecovery, never()).setStallThresholdMs(anyLong());
    }

    @Test
    public void testPatchInvalidHungRepairEnabledReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_recovery_enabled", "yes");

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("hung_repair_recovery_enabled must be a boolean");
        verify(myHungRepairSessionRecovery, never()).setEnabled(anyBoolean());
    }

    @Test
    public void testPatchHungRepairBypassCoordinatorCheck()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_bypass_coordinator_check", true);

        controller.patchConfig(patch);

        verify(myHungRepairSessionRecovery).setBypassCoordinatorCheck(true);
    }

    @Test
    public void testPatchInvalidHungRepairBypassReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_bypass_coordinator_check", "nope");

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error"))
                .isEqualTo("hung_repair_bypass_coordinator_check must be a boolean");
        verify(myHungRepairSessionRecovery, never()).setBypassCoordinatorCheck(anyBoolean());
    }

    @Test
    public void testPatchHungRepairForce()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_force", true);

        controller.patchConfig(patch);

        verify(myHungRepairSessionRecovery).setForce(true);
    }

    @Test
    public void testPatchInvalidHungRepairForceReturns400()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("hung_repair_force", "nope");

        ResponseEntity<Map<String, Object>> response = controller.patchConfig(patch);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody().get("error")).isEqualTo("hung_repair_force must be a boolean");
        verify(myHungRepairSessionRecovery, never()).setForce(anyBoolean());
    }
}
