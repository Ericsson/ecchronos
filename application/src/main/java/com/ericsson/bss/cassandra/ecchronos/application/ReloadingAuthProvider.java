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

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.function.Supplier;

import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ExtendedAuthProvider;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.ericsson.bss.cassandra.ecchronos.application.config.Credentials;

public class ReloadingAuthProvider implements ExtendedAuthProvider
{
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Supplier<Credentials> credentialSupplier;

    public ReloadingAuthProvider(Supplier<Credentials> credentialSupplier)
    {
        this.credentialSupplier = credentialSupplier;
    }

    @Override
    public Authenticator newAuthenticator(EndPoint endPoint, String authenticator) throws AuthenticationException
    {
        return new DefaultAuthenticator(credentialSupplier);
    }

    @Override
    public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException
    {
        throw new UnsupportedOperationException();
    }

    static class DefaultAuthenticator implements Authenticator
    {
        private final Supplier<Credentials> credentialsSupplier;

        DefaultAuthenticator(Supplier<Credentials> credentialsSupplier)
        {
            this.credentialsSupplier = credentialsSupplier;
        }

        @Override
        public byte[] initialResponse()
        {
            Credentials credentials = credentialsSupplier.get();
            if (!credentials.isEnabled())
            {
                return new byte[0];
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

                return response;
            }
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge)
        {
            return new byte[0];
        }

        @Override
        public void onAuthenticationSuccess(byte[] token)
        {

        }
    }
}
