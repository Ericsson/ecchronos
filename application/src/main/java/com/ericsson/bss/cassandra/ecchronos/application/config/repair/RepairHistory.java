/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

/** Configuration for the repair history provider. */
public class RepairHistory
{
    /** Defines the available repair history providers. */
    public enum Provider
    {
        /** The Cassandra provider. */
        CASSANDRA,
        /** The upgrade provider. */
        UPGRADE,
        /** The ECC provider. */
        ECC
    }

    private Provider myProvider = Provider.ECC;
    private String myKeyspaceName = "ecchronos";

    /** Default constructor. */
    public RepairHistory()
    {
    }

    /**
     * Returns the provider.
     * @return the provider
     */
    @JsonProperty("provider")
    public final Provider getProvider()
    {
        return myProvider;
    }

    /**
     * Sets the provider.
     * @param provider the provider implementation
     */
    @JsonProperty("provider")
    public final void setProvider(final String provider)
    {
        myProvider = Provider.valueOf(provider.toUpperCase(Locale.US));
    }

    /**
     * Returns the keyspace name.
     * @return the keyspace name
     */
    @JsonProperty("keyspace")
    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    /**
     * Sets the keyspace name.
     * @param keyspaceName the keyspace name
     */
    @JsonProperty("keyspace")
    public final void setKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
    }
}
