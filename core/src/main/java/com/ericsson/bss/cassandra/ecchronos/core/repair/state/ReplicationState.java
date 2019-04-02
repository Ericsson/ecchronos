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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class to generate a token -&gt; replicas map for a specific table.
 */
public class ReplicationState
{
    private static final Map<String, KeyspaceReplication> keyspaceReplicationCache = new ConcurrentHashMap<>();

    private final Metadata myMetadata;
    private final Host myLocalHost;

    public ReplicationState(Metadata metadata, Host localhost)
    {
        myMetadata = metadata;
        myLocalHost = localhost;
    }

    public Map<LongTokenRange, ImmutableSet<Host>> getTokenRangeToReplicas(TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicaMap = new HashMap<>();

        Map<ImmutableSet<Host>, ImmutableSet<Host>> replicaCache = new HashMap<>();

        for (TokenRange tokenRange : myMetadata.getTokenRanges(keyspace, myLocalHost))
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<Host> replicas = ImmutableSet.copyOf(myMetadata.getReplicas(keyspace, tokenRange));

            tokenRangeToReplicaMap.put(longTokenRange, getOrCache(replicas, replicaCache));
        }

        return getOrCache(keyspace, ImmutableMap.copyOf(tokenRangeToReplicaMap));
    }

    private ImmutableSet<Host> getOrCache(ImmutableSet<Host> replicas, Map<ImmutableSet<Host>, ImmutableSet<Host>> replicaCache)
    {
        ImmutableSet<Host> actualReplicas;
        actualReplicas = replicaCache.putIfAbsent(replicas, replicas);
        if (actualReplicas == null)
        {
            actualReplicas = replicas;
        }

        return actualReplicas;
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<Host>> getOrCache(String keyspace, ImmutableMap<LongTokenRange, ImmutableSet<Host>> replication)
    {
        KeyspaceReplication newKeyspaceReplication = new KeyspaceReplication(replication);
        KeyspaceReplication oldKeyspaceReplication = keyspaceReplicationCache.get(keyspace);

        if (oldKeyspaceReplication == null || !oldKeyspaceReplication.equals(newKeyspaceReplication))
        {
            keyspaceReplicationCache.put(keyspace, newKeyspaceReplication);
            return newKeyspaceReplication.getReplication();
        }

        return oldKeyspaceReplication.getReplication();
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }

    private static class KeyspaceReplication
    {
        private final ImmutableMap<LongTokenRange, ImmutableSet<Host>> myReplication;

        public KeyspaceReplication(ImmutableMap<LongTokenRange, ImmutableSet<Host>> replication)
        {
            myReplication = replication;
        }

        public ImmutableMap<LongTokenRange, ImmutableSet<Host>> getReplication()
        {
            return myReplication;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyspaceReplication that = (KeyspaceReplication) o;

            return myReplication != null ? myReplication.equals(that.myReplication) : that.myReplication == null;
        }

        @Override
        public int hashCode()
        {
            return myReplication != null ? myReplication.hashCode() : 0;
        }
    }
}
