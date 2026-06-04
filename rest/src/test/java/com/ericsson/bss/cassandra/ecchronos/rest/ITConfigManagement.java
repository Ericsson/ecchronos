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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairLockFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Integration test: round-trip GET/PATCH via ConfigManagementRESTImpl
 * with a real ScheduleManagerImpl to verify parameters are persisted and readable.
 */
public class ITConfigManagement
{
    private ConfigManagementRESTImpl myController;
    private ScheduleManagerImpl myScheduleManager;
    private int originalLocksPerResource;

    @Before
    public void setup()
    {
        UUID nodeId = UUID.randomUUID();
        Node mockNode = mock(Node.class);
        DistributedNativeConnectionProvider ncp = mock(DistributedNativeConnectionProvider.class);
        when(ncp.getNodes()).thenReturn(Map.of(nodeId, mockNode));
        CASLockFactory lockFactory = mock(CASLockFactory.class);

        myScheduleManager = ScheduleManagerImpl.builder()
                .withNodeIDList(Collections.singletonList(nodeId))
                .withNativeConnectionProvider(ncp)
                .withLockFactory(lockFactory)
                .build();

        myController = new ConfigManagementRESTImpl(myScheduleManager);
        originalLocksPerResource = RepairLockFactoryImpl.getLocksPerResource();
    }

    @After
    public void teardown()
    {
        myScheduleManager.close();
        RepairLockFactoryImpl.configure(originalLocksPerResource);
    }

    @Test
    public void testGetReturnsCurrentConfig()
    {
        ResponseEntity<Map<String, Object>> response = myController.getConfig();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        Map<String, Object> body = response.getBody();
        assertThat(((Number) body.get("session_window_ms")).longValue()).isEqualTo(300000L);
        assertThat(((Number) body.get("cooldown_ms")).longValue()).isEqualTo(0L);
        assertThat(((Number) body.get("locks_per_resource")).intValue()).isEqualTo(originalLocksPerResource);
    }

    @Test
    public void testPatchSessionWindowRoundTrip()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("session_window_ms", 120000L);

        myController.patchConfig(patch);

        ResponseEntity<Map<String, Object>> response = myController.getConfig();
        assertThat(((Number) response.getBody().get("session_window_ms")).longValue()).isEqualTo(120000L);
        assertThat(myScheduleManager.getSessionWindowInMs()).isEqualTo(120000L);
    }

    @Test
    public void testPatchCooldownRoundTrip()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("cooldown_ms", 15000L);

        myController.patchConfig(patch);

        assertThat(myScheduleManager.getCooldownInMs()).isEqualTo(15000L);
    }

    @Test
    public void testPatchLocksPerResourceRoundTrip()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("locks_per_resource", 5);

        myController.patchConfig(patch);

        assertThat(RepairLockFactoryImpl.getLocksPerResource()).isEqualTo(5);
    }

    @Test
    public void testPatchMultipleParametersRoundTrip()
    {
        Map<String, Object> patch = new HashMap<>();
        patch.put("session_window_ms", 600000L);
        patch.put("cooldown_ms", 5000L);
        patch.put("locks_per_resource", 2);

        ResponseEntity<Map<String, Object>> response = myController.patchConfig(patch);
        Map<String, Object> body = response.getBody();

        assertThat(((Number) body.get("session_window_ms")).longValue()).isEqualTo(600000L);
        assertThat(((Number) body.get("cooldown_ms")).longValue()).isEqualTo(5000L);
        assertThat(((Number) body.get("locks_per_resource")).intValue()).isEqualTo(2);
    }

    @Test
    public void testPatchDoesNotAffectUnspecifiedParameters()
    {
        long beforeSessionWindow = myScheduleManager.getSessionWindowInMs();

        Map<String, Object> patch = new HashMap<>();
        patch.put("cooldown_ms", 7000L);

        myController.patchConfig(patch);

        assertThat(myScheduleManager.getSessionWindowInMs()).isEqualTo(beforeSessionWindow);
        assertThat(RepairLockFactoryImpl.getLocksPerResource()).isEqualTo(originalLocksPerResource);
    }
}
