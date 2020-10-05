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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ExtendedAuthProvider;
import com.ericsson.bss.cassandra.ecchronos.application.config.Credentials;

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

    @Before
    public void setup()
    {
        when(endPoint.resolve()).thenReturn(new InetSocketAddress(0));
    }

    @Test
    public void testCorrectResponse()
    {
        ByteBuffer expectedResponse = responseFor("username", "password");

        when(credentialsSupplier.get()).thenReturn(new Credentials(true, "username", "password"));
        ExtendedAuthProvider authProvider = new ReloadingAuthProvider(credentialsSupplier);

        Authenticator authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        ByteBuffer response = ByteBuffer.wrap(authenticator.initialResponse());
        assertThat(response).isEqualTo(expectedResponse);
        assertThat(getUsername(response)).isEqualTo("username");
        assertThat(getPassword(response)).isEqualTo("password");
    }

    @Test
    public void testChangingResponse()
    {
        ByteBuffer expectedResponse = responseFor("username", "password");
        ByteBuffer expectedResponse2 = responseFor("new_user", "new_password");

        when(credentialsSupplier.get())
                .thenReturn(new Credentials(true, "username", "password"))
                .thenReturn(new Credentials(true, "new_user", "new_password"));
        ExtendedAuthProvider authProvider = new ReloadingAuthProvider(credentialsSupplier);

        Authenticator authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        ByteBuffer response = ByteBuffer.wrap(authenticator.initialResponse());
        assertThat(response).isEqualTo(expectedResponse);
        assertThat(getUsername(response)).isEqualTo("username");
        assertThat(getPassword(response)).isEqualTo("password");

        authenticator = authProvider.newAuthenticator(endPoint, SERVER_AUTHENTICATOR);

        response = ByteBuffer.wrap(authenticator.initialResponse());
        assertThat(response).isEqualTo(expectedResponse2);
        assertThat(getUsername(response)).isEqualTo("new_user");
        assertThat(getPassword(response)).isEqualTo("new_password");
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

    private ByteBuffer responseFor(String username, String password)
    {
        byte[] user = username.getBytes(UTF8);
        byte[] pass = password.getBytes(UTF8);
        ByteBuffer response = ByteBuffer.allocate(user.length + pass.length + 2);
        response.put((byte) 0);
        response.put(user);
        response.put((byte) 0);
        response.put(pass);
        response.rewind();

        return response;
    }
}
