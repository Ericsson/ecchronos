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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairHistoryProvider;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

public class RepairHistory
{
    private RepairHistoryProvider myProvider = RepairHistoryProvider.ECC;
    private String myKeyspaceName = "ecchronos";

    @JsonProperty("provider")
    public final RepairHistoryProvider getProvider()
    {
        return myProvider;
    }

    @JsonProperty("provider")
    public final void setProvider(final String provider)
    {
        myProvider = RepairHistoryProvider.valueOf(provider.toUpperCase(Locale.US));
    }

    @JsonProperty("keyspace")
    public final String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    @JsonProperty("keyspace")
    public final void setKeyspaceName(final String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
    }
}

