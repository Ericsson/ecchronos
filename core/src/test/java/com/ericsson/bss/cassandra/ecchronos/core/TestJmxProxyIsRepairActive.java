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
package com.ericsson.bss.cassandra.ecchronos.core;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestJmxProxyIsRepairActive
{
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    private static final int COMMAND = 42;

    @Mock
    private JmxConnectionProvider mockConnectionProvider;

    @Mock
    private JMXConnector mockJmxConnector;

    @Mock
    private MBeanServerConnection mockMbeanServer;

    private JmxProxy proxy;

    @Before
    public void init() throws Exception
    {
        when(mockConnectionProvider.getJmxConnector()).thenReturn(mockJmxConnector);
        when(mockJmxConnector.getMBeanServerConnection()).thenReturn(mockMbeanServer);

        proxy = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(mockConnectionProvider)
                .build()
                .connect();
    }

    @Test
    public void testInProgressReturnsTrue() throws Exception
    {
        when(mockMbeanServer.invoke(
                eq(new ObjectName(SS_OBJ_NAME)),
                eq("getParentRepairStatus"),
                eq(new Object[]{COMMAND}),
                any(String[].class)))
                .thenReturn(Arrays.asList("IN_PROGRESS"));

        assertThat(proxy.isRepairActive(COMMAND)).isTrue();
    }

    @Test
    public void testCompletedReturnsFalse() throws Exception
    {
        when(mockMbeanServer.invoke(
                eq(new ObjectName(SS_OBJ_NAME)),
                eq("getParentRepairStatus"),
                eq(new Object[]{COMMAND}),
                any(String[].class)))
                .thenReturn(Arrays.asList("COMPLETED", "Repair completed successfully"));

        assertThat(proxy.isRepairActive(COMMAND)).isFalse();
    }

    @Test
    public void testFailedReturnsFalse() throws Exception
    {
        when(mockMbeanServer.invoke(
                eq(new ObjectName(SS_OBJ_NAME)),
                eq("getParentRepairStatus"),
                eq(new Object[]{COMMAND}),
                any(String[].class)))
                .thenReturn(Arrays.asList("FAILED", "Repair failed due to error"));

        assertThat(proxy.isRepairActive(COMMAND)).isFalse();
    }

    @Test
    public void testNullStatusReturnsFalse() throws Exception
    {
        when(mockMbeanServer.invoke(
                eq(new ObjectName(SS_OBJ_NAME)),
                eq("getParentRepairStatus"),
                eq(new Object[]{COMMAND}),
                any(String[].class)))
                .thenReturn(null);

        assertThat(proxy.isRepairActive(COMMAND)).isFalse();
    }

    @Test
    public void testExceptionReturnsTrue() throws Exception
    {
        when(mockMbeanServer.invoke(
                eq(new ObjectName(SS_OBJ_NAME)),
                eq("getParentRepairStatus"),
                eq(new Object[]{COMMAND}),
                any(String[].class)))
                .thenThrow(new RuntimeException("JMX connection lost"));

        assertThat(proxy.isRepairActive(COMMAND)).isTrue();
    }
}
