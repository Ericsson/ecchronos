/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;

public class ReplicatedTableProviderImpl implements ReplicatedTableProvider
{
    private static final String SYSTEM_AUTH_KEYSPACE = "system_auth";

    private final Host myLocalhost;
    private final Metadata myMetadata;

    public ReplicatedTableProviderImpl(Host host, Metadata metadata)
    {
        myLocalhost = host;
        myMetadata = metadata;
    }

    @Override
    public Set<TableReference> getAll()
    {
        return myMetadata.getKeyspaces().stream()
                .filter(k -> accept(k.getName()))
                .flatMap(k -> k.getTables().stream())
                .map(tb -> new TableReference(tb.getKeyspace().getName(), tb.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public boolean accept(String keyspace)
    {
        if (keyspace.startsWith("system") && !SYSTEM_AUTH_KEYSPACE.equals(keyspace))
        {
            return false;
        }

        Set<TokenRange> tokenRanges = myMetadata.getTokenRanges(keyspace, myLocalhost);

        if (tokenRanges.isEmpty())
        {
            return false;
        }

        TokenRange range = tokenRanges.iterator().next();
        return myMetadata.getReplicas(keyspace, range).size() >= 2;
    }
}
