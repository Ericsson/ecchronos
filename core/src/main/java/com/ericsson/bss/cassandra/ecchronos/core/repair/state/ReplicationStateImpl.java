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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Utility class to generate a token -&gt; replicas map for a specific table.
 */
public class ReplicationStateImpl implements ReplicationState
{
    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<Host>>> keyspaceReplicationCache = new ConcurrentHashMap<>();

    private final Metadata myMetadata;
    private final Host myLocalHost;

    public ReplicationStateImpl(Metadata metadata, Host localhost)
    {
        myMetadata = metadata;
        myLocalHost = localhost;
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<Host>> getTokenRangeToReplicas(TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<Host>> replication = buildTokenMap(keyspace);

        return keyspaceReplicationCache.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<Host>> buildTokenMap(String keyspace)
    {
        ImmutableMap.Builder<LongTokenRange, ImmutableSet<Host>> replicationBuilder = ImmutableMap.builder();

        Map<Set<Host>, ImmutableSet<Host>> replicaCache = new HashMap<>();

        for (TokenRange tokenRange : myMetadata.getTokenRanges(keyspace, myLocalHost))
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<Host> replicas = replicaCache.computeIfAbsent(myMetadata.getReplicas(keyspace, tokenRange),
                    ImmutableSet::copyOf);

            replicationBuilder.put(longTokenRange, replicas);
        }

        return replicationBuilder.build();
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
