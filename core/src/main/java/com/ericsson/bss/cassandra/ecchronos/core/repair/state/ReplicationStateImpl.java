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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Utility class to generate a token -&gt; replicas map for a specific table.
 */
public class ReplicationStateImpl implements ReplicationState
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateImpl.class);

    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<Node>>> keyspaceReplicationCache = new ConcurrentHashMap<>();

    private final NodeResolver myNodeResolver;
    private final Metadata myMetadata;
    private final Host myLocalHost;

    public ReplicationStateImpl(NodeResolver nodeResolver, Metadata metadata, Host localhost)
    {
        myNodeResolver = nodeResolver;
        myMetadata = metadata;
        myLocalHost = localhost;
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<Node>> getTokenRangeToReplicas(TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<Node>> replication = buildTokenMap(keyspace);

        return keyspaceReplicationCache.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<Node>> buildTokenMap(String keyspace)
    {
        ImmutableMap.Builder<LongTokenRange, ImmutableSet<Node>> replicationBuilder = ImmutableMap.builder();

        Map<Set<Host>, ImmutableSet<Node>> replicaCache = new HashMap<>();

        for (TokenRange tokenRange : myMetadata.getTokenRanges(keyspace, myLocalHost))
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<Node> replicas = replicaCache.computeIfAbsent(myMetadata.getReplicas(keyspace, tokenRange),
                    this::convert);

            replicationBuilder.put(longTokenRange, replicas);
        }

        return replicationBuilder.build();
    }

    private ImmutableSet<Node> convert(Set<Host> hosts)
    {
        ImmutableSet.Builder<Node> builder = new ImmutableSet.Builder<>();
        for (Host host : hosts)
        {
            Optional<Node> node = myNodeResolver.fromIp(host.getBroadcastAddress());
            if (node.isPresent())
            {
                builder.add(node.get());
            }
            else
            {
                LOG.warn("Node {} - {} not found in node resolver", host.getHostId(), host.getBroadcastAddress());
            }
        }
        return builder.build();
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
