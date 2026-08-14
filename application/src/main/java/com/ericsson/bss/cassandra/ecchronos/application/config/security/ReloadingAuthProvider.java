/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.metadata.EndPoint;

import java.util.function.Supplier;

/**
 * An authentication provider that reloads credentials from a supplier on each authentication request.
 * Extends {@link ProgrammaticPlainTextAuthProvider} to support dynamically updated credentials
 * without requiring application restart.
 */
public class ReloadingAuthProvider extends ProgrammaticPlainTextAuthProvider
{
    private final Supplier<com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials>
            credentialSupplier;

    /**
     * Constructs a new ReloadingAuthProvider with the given credential supplier.
     *
     * @param aCredentialSupplier a supplier that provides the current credentials
     */
    public ReloadingAuthProvider(
            final Supplier<com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials>
                    aCredentialSupplier)
    {
        super(aCredentialSupplier.get().getUsername(), aCredentialSupplier.get().getPassword());
        this.credentialSupplier = aCredentialSupplier;
    }

    @Override
    protected final Credentials getCredentials(final EndPoint endPoint, final String serverAuthenticator)
    {
        com.ericsson.bss.cassandra.ecchronos.application.config.security.Credentials credentials =
                credentialSupplier.get();
        return new Credentials(credentials.getUsername().toCharArray(), credentials.getPassword().toCharArray());
    }
}
