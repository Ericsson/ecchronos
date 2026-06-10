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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.http.HttpClient;

public class TestJolokiaHttpClientCertReload
{
    @Test
    public void testHttpClientRebuildsWhenSSLContextChanges() throws Exception
    {
        CertificateHandler certHandler = mock(CertificateHandler.class);
        DistributedNativeConnectionProvider ncp = mock(DistributedNativeConnectionProvider.class);

        SSLContext ctx1 = SSLContext.getInstance("TLS");
        ctx1.init(null, null, null);
        SSLContext ctx2 = SSLContext.getInstance("TLS");
        ctx2.init(null, null, null);

        // Initial context
        when(certHandler.getSSLContext()).thenReturn(ctx1);

        JolokiaNotificationController controller = new JolokiaNotificationController.Builder()
                .withNativeConnection(ncp)
                .withCertificateHandler(certHandler)
                .withJolokiaPort(8778)
                .withJolokiaPEM(true)
                .withRunDelay(500)
                .build();

        HttpClient client1 = controller.getHttpClient();
        assertThat(client1).isNotNull();

        // Same context — no rebuild
        HttpClient client1Again = controller.getHttpClient();
        assertThat(client1Again).isSameAs(client1);

        // New context — should rebuild
        when(certHandler.getSSLContext()).thenReturn(ctx2);
        HttpClient client2 = controller.getHttpClient();
        assertThat(client2).isNotSameAs(client1);

        // Same new context — no rebuild
        HttpClient client2Again = controller.getHttpClient();
        assertThat(client2Again).isSameAs(client2);
    }

    @Test
    public void testHttpClientNotRebuiltWhenNoCertHandler()
    {
        DistributedNativeConnectionProvider ncp = mock(DistributedNativeConnectionProvider.class);

        JolokiaNotificationController controller = new JolokiaNotificationController.Builder()
                .withNativeConnection(ncp)
                .withJolokiaPort(8778)
                .withJolokiaPEM(false)
                .withRunDelay(500)
                .build();

        HttpClient client1 = controller.getHttpClient();
        HttpClient client2 = controller.getHttpClient();
        assertThat(client2).isSameAs(client1);
    }
}
