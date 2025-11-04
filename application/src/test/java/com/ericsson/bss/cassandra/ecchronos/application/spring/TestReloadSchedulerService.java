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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.AgentConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.DistributedNativeConnection;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.RetryPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;

@RunWith(MockitoJUnitRunner.class)
public class TestReloadSchedulerService
{
    @Mock
    private Config mockConfig;

    @Mock
    private DistributedNativeConnectionProvider mockConnectionProvider;

    @Mock
    private DefaultRepairConfigurationProvider mockRepairConfigProvider;

    @Mock
    private ConnectionConfig mockConnectionConfig;

    @Mock
    private DistributedNativeConnection mockCqlConnectionConfig;

    @Mock
    private AgentConnectionConfig mockAgentConnectionConfig;

    @Mock
    private RetryPolicyConfig.RetrySchedule mockReloadSchedule;

    @Mock
    private Node mockNode1;

    @Mock
    private Node mockNode2;

    private ReloadSchedulerService reloadSchedulerService;
    private UUID nodeId1 = UUID.randomUUID();
    private UUID nodeId2 = UUID.randomUUID();

    @Before
    public void setup()
    {
        when(mockConfig.getConnectionConfig()).thenReturn(mockConnectionConfig);
        when(mockConnectionConfig.getCqlConnection()).thenReturn(mockCqlConnectionConfig);
        when(mockCqlConnectionConfig.getAgentConnectionConfig()).thenReturn(mockAgentConnectionConfig);
        when(mockAgentConnectionConfig.getReloadSchedule()).thenReturn(mockReloadSchedule);

        when(mockReloadSchedule.getInitialDelay()).thenReturn(2000L);
        when(mockReloadSchedule.getFixedDelay()).thenReturn(10000L);
        when(mockReloadSchedule.getUnit()).thenReturn(TimeUnit.MILLISECONDS);

        reloadSchedulerService = new ReloadSchedulerService(mockConfig, mockConnectionProvider, mockRepairConfigProvider);
    }

    @Test
    public void testConstructorInitializesCorrectly()
    {
        assertNotNull(reloadSchedulerService);
        verify(mockConfig).getConnectionConfig();
        verify(mockConnectionConfig).getCqlConnection();
        verify(mockCqlConnectionConfig).getAgentConnectionConfig();
        verify(mockAgentConnectionConfig).getReloadSchedule();
    }

    @Test
    public void testStartSchedulerCallsScheduleWithCorrectParameters()
    {
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        
        try
        {
            Field schedulerField = ReloadSchedulerService.class.getDeclaredField("myScheduler");
            schedulerField.setAccessible(true);
            schedulerField.set(reloadSchedulerService, mockScheduler);
            
            reloadSchedulerService.startScheduler();
            
            verify(mockScheduler).scheduleWithFixedDelay(any(Runnable.class), eq(2000L), eq(10000L), eq(TimeUnit.MILLISECONDS));
        }
        catch (Exception e)
        {
            fail("Failed to test startScheduler method: " + e.getMessage());
        }
    }

    @Test
    public void testCompareNodesMapAddsNewNode()
    {
        Map<UUID, Node> existingNodes = new HashMap<>();
        when(mockConnectionProvider.getNodes()).thenReturn(existingNodes);

        Map<UUID, Node> newNodes = new HashMap<>();
        newNodes.put(nodeId1, mockNode1);

        try
        {
            Method method = ReloadSchedulerService.class.getDeclaredMethod("compareNodesMap", Map.class);
            method.setAccessible(true);
            method.invoke(reloadSchedulerService, newNodes);

            verify(mockRepairConfigProvider).onAdd(mockNode1);
        }
        catch (Exception e)
        {
            fail("Failed to invoke compareNodesMap method: " + e.getMessage());
        }
    }

    @Test
    public void testCompareNodesMapRemovesOldNode()
    {
        Map<UUID, Node> existingNodes = new HashMap<>();
        existingNodes.put(nodeId1, mockNode1);
        when(mockConnectionProvider.getNodes()).thenReturn(existingNodes);

        Map<UUID, Node> newNodes = new HashMap<>();

        try
        {
            Method method = ReloadSchedulerService.class.getDeclaredMethod("compareNodesMap", Map.class);
            method.setAccessible(true);
            method.invoke(reloadSchedulerService, newNodes);

            verify(mockRepairConfigProvider).onRemove(mockNode1);
        }
        catch (Exception e)
        {
            fail("Failed to invoke compareNodesMap method: " + e.getMessage());
        }
    }

    @Test
    public void testCompareNodesMapHandlesBothAddAndRemove()
    {
        Map<UUID, Node> existingNodes = new HashMap<>();
        existingNodes.put(nodeId1, mockNode1);
        when(mockConnectionProvider.getNodes()).thenReturn(existingNodes);

        Map<UUID, Node> newNodes = new HashMap<>();
        newNodes.put(nodeId2, mockNode2);

        try
        {
            Method method = ReloadSchedulerService.class.getDeclaredMethod("compareNodesMap", Map.class);
            method.setAccessible(true);
            method.invoke(reloadSchedulerService, newNodes);

            verify(mockRepairConfigProvider).onAdd(mockNode2);
            verify(mockRepairConfigProvider).onRemove(mockNode1);
        }
        catch (Exception e)
        {
            fail("Failed to invoke compareNodesMap method: " + e.getMessage());
        }
    }

    @Test
    public void testCompareNodesMapNoChanges()
    {
        Map<UUID, Node> existingNodes = new HashMap<>();
        existingNodes.put(nodeId1, mockNode1);
        when(mockConnectionProvider.getNodes()).thenReturn(existingNodes);

        Map<UUID, Node> newNodes = new HashMap<>();
        newNodes.put(nodeId1, mockNode1);

        try
        {
            Method method = ReloadSchedulerService.class.getDeclaredMethod("compareNodesMap", Map.class);
            method.setAccessible(true);
            method.invoke(reloadSchedulerService, newNodes);

            verify(mockRepairConfigProvider, never()).onAdd(any());
            verify(mockRepairConfigProvider, never()).onRemove(any());
        }
        catch (Exception e)
        {
            fail("Failed to invoke compareNodesMap method: " + e.getMessage());
        }
    }

    @Test
    public void testDestroyShutdownsScheduler() throws InterruptedException
    {
        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        when(mockScheduler.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);

        try
        {
            Field schedulerField = ReloadSchedulerService.class.getDeclaredField("myScheduler");
            schedulerField.setAccessible(true);
            schedulerField.set(reloadSchedulerService, mockScheduler);

            reloadSchedulerService.destroy();

            verify(mockScheduler).shutdown();
            verify(mockScheduler).awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            fail("Failed to test destroy method: " + e.getMessage());
        }
    }
}
