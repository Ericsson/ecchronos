/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestReloadingAuthProvider
{
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final int POS_USERNAME = 1;
    private static final int POS_PASSWORD = 2;

    private static final String SERVER_AUTHENTICATOR = "org.apache.cassandra.auth.PasswordAuthenticator";

    @Mock
    private Supplier<Credentials> credentialsSupplier;

    @Mock
    private EndPoint endPoint;

    @Test
    public void testCorrectResponse()
    {
        when(credentialsSupplier.get()).thenReturn(new Credentials(true, "username", "password"));
        AuthProvider authProvider = new ReloadingAuthProvider(credentialsSupplier);

        Authenticator authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        authenticator.initialResponse().thenAccept(b ->
        {
            assertThat(getUsername(b)).isEqualTo("username");
            assertThat(getPassword(b)).isEqualTo("password");
        });
    }

    @Test
    public void testChangingResponse()
    {
        when(credentialsSupplier.get())
                .thenReturn(new Credentials(true, "username", "password"))
                .thenReturn(new Credentials(true, "new_user", "new_password"));
        AuthProvider authProvider = new ReloadingAuthProvider(credentialsSupplier);

        Authenticator authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        authenticator.initialResponse().thenAccept(b ->
        {
            assertThat(getUsername(b)).isEqualTo("username");
            assertThat(getPassword(b)).isEqualTo("password");
        });

        authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        authenticator.initialResponse().thenAccept(b ->
        {
            assertThat(getUsername(b)).isEqualTo("new_user");
            assertThat(getPassword(b)).isEqualTo("new_password");
        });
    }

    private String getUsername(ByteBuffer response)
    {
        return readField(response, POS_USERNAME);
    }

    private String getPassword(ByteBuffer response)
    {
        return readField(response, POS_PASSWORD);
    }

    private String readField(ByteBuffer response, int pos)
    {
        ByteBuffer buffer = readUntil(response, pos);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (buffer.hasRemaining())
        {
            byte next = buffer.get();
            if (next == 0)
            {
                break;
            }
            baos.write(next);
        }
        return new String(baos.toByteArray(), UTF8);
    }

    private ByteBuffer readUntil(ByteBuffer response, int pos)
    {
        ByteBuffer result = response.duplicate();
        result.rewind();

        int zeroFound = 0;

        // Read the buffer until we are at the expected position
        while (zeroFound < pos && result.hasRemaining())
        {
            if (result.get() == 0)
            {
                zeroFound++;
            }
        }

        return result;
    }
}
