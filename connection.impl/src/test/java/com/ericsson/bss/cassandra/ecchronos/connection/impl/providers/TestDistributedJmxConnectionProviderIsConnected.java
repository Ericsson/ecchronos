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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.providers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import org.junit.Test;

/**
 * Tests for {@link DistributedJmxConnectionProviderImpl#isConnected(JMXConnector)}.
 * Verifies the active liveness probe using getMBeanCount().
 */
public class TestDistributedJmxConnectionProviderIsConnected
{
    private final DistributedJmxConnectionProviderImpl provider =
            new DistributedJmxConnectionProviderImpl(
                    DistributedJmxConnectionProviderImpl.builder());

    @Test
    public void testNullConnectorReturnsFalse()
    {
        assertThat(provider.isConnected((JMXConnector) null)).isFalse();
    }

    @Test
    public void testHealthyConnectionReturnsTrue() throws IOException
    {
        JMXConnector connector = mock(JMXConnector.class);
        MBeanServerConnection mbs = mock(MBeanServerConnection.class);
        when(connector.getConnectionId()).thenReturn("test-connection-id");
        when(connector.getMBeanServerConnection()).thenReturn(mbs);
        when(mbs.getMBeanCount()).thenReturn(42);

        assertThat(provider.isConnected(connector)).isTrue();
    }

    @Test
    public void testNullMBeanServerConnectionReturnsFalse() throws IOException
    {
        JMXConnector connector = mock(JMXConnector.class);
        when(connector.getConnectionId()).thenReturn("test-connection-id");
        when(connector.getMBeanServerConnection()).thenReturn(null);

        assertThat(provider.isConnected(connector)).isFalse();
    }

    @Test
    public void testGetConnectionIdThrowsReturnsFalse() throws IOException
    {
        JMXConnector connector = mock(JMXConnector.class);
        when(connector.getConnectionId()).thenThrow(new IOException("connection lost"));

        assertThat(provider.isConnected(connector)).isFalse();
    }

    @Test
    public void testGetMBeanCountThrowsIOExceptionReturnsFalse() throws IOException
    {
        JMXConnector connector = mock(JMXConnector.class);
        MBeanServerConnection mbs = mock(MBeanServerConnection.class);
        when(connector.getConnectionId()).thenReturn("test-connection-id");
        when(connector.getMBeanServerConnection()).thenReturn(mbs);
        when(mbs.getMBeanCount()).thenThrow(new IOException("Jolokia endpoint unreachable"));

        assertThat(provider.isConnected(connector)).isFalse();
    }

    @Test
    public void testGetMBeanServerConnectionThrowsReturnsFalse() throws IOException
    {
        JMXConnector connector = mock(JMXConnector.class);
        when(connector.getConnectionId()).thenReturn("test-connection-id");
        when(connector.getMBeanServerConnection()).thenThrow(new IOException("stale connection"));

        assertThat(provider.isConnected(connector)).isFalse();
    }
}
