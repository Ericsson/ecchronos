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

import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.metadata.EndPoint;

import java.util.function.Supplier;

public class ReloadingAuthProvider extends ProgrammaticPlainTextAuthProvider
{
    private final Supplier<com.ericsson.bss.cassandra.ecchronos.application.config.Credentials> credentialSupplier;

    public ReloadingAuthProvider(
            Supplier<com.ericsson.bss.cassandra.ecchronos.application.config.Credentials> credentialSupplier)
    {
        super(credentialSupplier.get().getUsername(), credentialSupplier.get().getPassword());
        this.credentialSupplier = credentialSupplier;
    }

    @Override
    protected Credentials getCredentials(EndPoint endPoint, String serverAuthenticator)
    {
        com.ericsson.bss.cassandra.ecchronos.application.config.Credentials credentials = credentialSupplier.get();
        return new Credentials(credentials.getUsername().toCharArray(), credentials.getPassword().toCharArray());
    }
}
