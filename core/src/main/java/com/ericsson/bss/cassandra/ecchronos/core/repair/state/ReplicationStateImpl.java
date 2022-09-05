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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
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

    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>>> keyspaceReplicationCache = new ConcurrentHashMap<>();
    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>>> clusterWideKeyspaceReplicationCache = new ConcurrentHashMap<>();

    private final NodeResolver myNodeResolver;
    private final CqlSession mySession;
    private final Node myLocalNode;

    public ReplicationStateImpl(NodeResolver nodeResolver, CqlSession session, Node localNode)
    {
        myNodeResolver = nodeResolver;
        mySession = session;
        myLocalNode = localNode;
    }

    @Override
    public ImmutableSet<DriverNode> getNodes(TableReference tableReference, LongTokenRange tokenRange)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = maybeRenew(keyspace);
        return getNodes(replication, tokenRange);
    }

    @Override
    public ImmutableSet<DriverNode> getNodesClusterWide(TableReference tableReference, LongTokenRange tokenRange)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = maybeRenewClusterWide(keyspace);
        return getNodes(replication, tokenRange);
    }

    private ImmutableSet<DriverNode> getNodes(ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication, LongTokenRange tokenRange)
    {
        ImmutableSet<DriverNode> nodes = replication.get(tokenRange);

        if (nodes == null)
        {
            for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> entry : replication.entrySet())
            {
                if (entry.getKey().isCovering(tokenRange))
                {
                    nodes = entry.getValue();
                    break;
                }
            }
        }

        return nodes;
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<DriverNode>> getTokenRangeToReplicas(TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenew(keyspace);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> maybeRenew(String keyspace)
    {
        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = buildTokenMap(keyspace, false);

        return keyspaceReplicationCache.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<DriverNode>> getTokenRanges(TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenewClusterWide(keyspace);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> maybeRenewClusterWide(String keyspace)
    {
        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = buildTokenMap(keyspace, true);

        return clusterWideKeyspaceReplicationCache.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> buildTokenMap(String keyspace, boolean clusterWide)
    {
        ImmutableMap.Builder<LongTokenRange, ImmutableSet<DriverNode>> replicationBuilder = ImmutableMap.builder();
        Map<Set<Node>, ImmutableSet<DriverNode>> replicaCache = new HashMap<>();
        Metadata metadata = mySession.getMetadata();
        Optional<TokenMap> tokenMap = metadata.getTokenMap();
        if (!tokenMap.isPresent())
        {
            throw new IllegalStateException("Cannot determine ranges, is metadata/tokenMap disabled?");
        }
        Set<TokenRange> tokenRanges;
        if (clusterWide)
        {
            tokenRanges = tokenMap.get().getTokenRanges();
        }
        else
        {
            tokenRanges = tokenMap.get().getTokenRanges(keyspace, myLocalNode);
        }
        for (TokenRange tokenRange : tokenRanges)
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<DriverNode> replicas = replicaCache.computeIfAbsent(tokenMap.get().getReplicas(keyspace, tokenRange),
                    this::convert);

            replicationBuilder.put(longTokenRange, replicas);
        }

        return replicationBuilder.build();
    }

    private ImmutableSet<DriverNode> convert(Set<Node> nodes)
    {
        ImmutableSet.Builder<DriverNode> builder = new ImmutableSet.Builder<>();
        for (Node node : nodes)
        {
            Optional<InetSocketAddress> broadcastAddress = node.getBroadcastAddress();
            if (broadcastAddress.isPresent())
            {
                Optional<DriverNode> resolvedNode = myNodeResolver.fromIp(broadcastAddress.get().getAddress());
                if (resolvedNode.isPresent())
                {
                    builder.add(resolvedNode.get());
                }
                else
                {
                    LOG.warn("Node {} - {} not found in node resolver", node.getHostId(), broadcastAddress.get());
                }
            }
            else
            {
                LOG.warn("Could not determine broadcast address for node {}", node.getHostId());
            }
        }
        return builder.build();
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
