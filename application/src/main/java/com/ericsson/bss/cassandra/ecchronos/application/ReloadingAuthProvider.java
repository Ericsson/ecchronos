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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.application.config.Credentials;

public class ReloadingAuthProvider implements AuthProvider
{
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Supplier<Credentials> credentialSupplier;

    public ReloadingAuthProvider(Supplier<Credentials> credentialSupplier)
    {
        this.credentialSupplier = credentialSupplier;
    }

    @Override
    public void onMissingChallenge(EndPoint endPoint) throws AuthenticationException
    {

    }

    @Override
    public Authenticator newAuthenticator(EndPoint endPoint, String serverAuthenticator) throws AuthenticationException
    {
        return new DefaultAuthenticator(credentialSupplier);
    }

    @Override public void close() throws Exception
    {

    }

    static class DefaultAuthenticator implements Authenticator
    {
        private final Supplier<Credentials> credentialsSupplier;

        DefaultAuthenticator(Supplier<Credentials> credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
        }

        @Override
        public CompletionStage<ByteBuffer> initialResponse()
        {
            return CompletableFutures.wrap(this::initialResponseAsync);
        }

        private ByteBuffer initialResponseAsync()
        {
            Credentials credentials = credentialsSupplier.get();
            if (!credentials.isEnabled())
            {
                return ByteBuffer.wrap(new byte[]{});
            }
            else
            {
                byte[] username = credentials.getUsername().getBytes(UTF8);
                byte[] password = credentials.getPassword().getBytes(UTF8);

                // '0' username '0' password
                byte[] response = new byte[1 + username.length + password.length + 1];
                response[0] = 0;
                System.arraycopy(username, 0, response, 1, username.length);
                response[username.length + 1] = 0;
                System.arraycopy(password, 0, response, username.length + 2, password.length);
                return ByteBuffer.wrap(response);
            }
        }

        @Override
        public CompletionStage<ByteBuffer> evaluateChallenge(ByteBuffer challenge)
        {
            return null;
        }

        @Override
        public CompletionStage<Void> onAuthenticationSuccess(ByteBuffer token)
        {
            return null;
        }
    }
}
