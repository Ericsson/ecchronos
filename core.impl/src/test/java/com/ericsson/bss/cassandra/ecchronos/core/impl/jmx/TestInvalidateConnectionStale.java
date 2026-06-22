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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.util.Map;
import java.util.UUID;

public class TestInvalidateConnectionStale
{
    private DistributedJmxConnectionProvider myConnectionProvider;
    private EccNodesSync myEccNodesSync;
    private RMIJmxProxy myProxy;
    private UUID myNodeId;

    @Before
    public void setup() throws Exception
    {
        myConnectionProvider = mock(DistributedJmxConnectionProvider.class);
        myEccNodesSync = mock(EccNodesSync.class);
        myNodeId = UUID.randomUUID();
        when(myConnectionProvider.getJmxConnections()).thenReturn(new java.util.concurrent.ConcurrentHashMap<>());

        myProxy = new RMIJmxProxy(myConnectionProvider, Map.of(), myEccNodesSync);
    }

    @Test
    public void testInvalidatesOnHttpConnectTimeout() throws IOException
    {
        Exception wrapper = new RuntimeException(new HttpConnectTimeoutException("timed out"));
        myProxy.invalidateIfConnectionStale(myNodeId, wrapper);

        verify(myConnectionProvider).close(myNodeId);
    }

    @Test
    public void testInvalidatesOnConnectException() throws IOException
    {
        Exception wrapper = new RuntimeException(new ConnectException("Connection refused"));
        myProxy.invalidateIfConnectionStale(myNodeId, wrapper);

        verify(myConnectionProvider).close(myNodeId);
    }

    @Test
    public void testDoesNotInvalidateOnUnrelatedIOException() throws IOException
    {
        Exception wrapper = new RuntimeException(new IOException("generic IO error"));
        myProxy.invalidateIfConnectionStale(myNodeId, wrapper);

        verify(myConnectionProvider, never()).close(myNodeId);
    }

    @Test
    public void testDoesNotInvalidateOnNullPointerException() throws IOException
    {
        myProxy.invalidateIfConnectionStale(myNodeId, new NullPointerException());

        verify(myConnectionProvider, never()).close(myNodeId);
    }
}
