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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
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

import static com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata.quoteIfNeeded;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Utility class to generate a token -&gt; replicas map for a specific table.
 */
public class ReplicationStateImpl implements ReplicationState
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateImpl.class);

    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>>>
            KEYSPACE_REPLICATION_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>>>
            CLUSTER_WIDE_KEYSPACE_REPLICATION_CACHE = new ConcurrentHashMap<>();

    private final NodeResolver myNodeResolver;
    private final CqlSession mySession;

    public ReplicationStateImpl(final NodeResolver nodeResolver, final CqlSession session)
    {
        myNodeResolver = nodeResolver;
        mySession = session;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableSet<DriverNode> getNodes(
            final TableReference tableReference,
            final LongTokenRange tokenRange,
            final Node currentNode)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = maybeRenew(keyspace, currentNode);
        return getNodes(replication, tokenRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableSet<DriverNode> getReplicas(
            final TableReference tableReference,
            final Node currentNode)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokens = getTokenRangeToReplicas(tableReference, currentNode);
        Set<DriverNode> allReplicas = new HashSet<>();
        for (ImmutableSet<DriverNode> replicas : tokens.values())
        {
            allReplicas.addAll(replicas);
        }
        return ImmutableSet.copyOf(allReplicas);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableSet<DriverNode> getNodesClusterWide(
            final TableReference tableReference,
            final LongTokenRange tokenRange,
            final Node currentNode)
    {
        String keyspace = tableReference.getKeyspace();

        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = maybeRenewClusterWide(keyspace, currentNode);
        return getNodes(replication, tokenRange);
    }

    private ImmutableSet<DriverNode> getNodes(final ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication,
            final LongTokenRange tokenRange)
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

    /**
     * Get token ranges to replicas.
     *
     * @param tableReference
     *            The table used to calculate the proper replication.
     * @return Nodes and their ranges
     */
    @Override
    public Map<LongTokenRange, ImmutableSet<DriverNode>> getTokenRangeToReplicas(
            final TableReference tableReference,
            final Node currentNode)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenew(keyspace, currentNode);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> maybeRenew(
            final String keyspace,
            final Node currentNode)
    {
        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = buildTokenMap(
                keyspace,
                false,
                currentNode);

        return KEYSPACE_REPLICATION_CACHE.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    /**
     * Get token ranges.
     *
     * @param tableReference Table reference.
     * @return Nodes and their ranges
     */
    @Override
    public Map<LongTokenRange, ImmutableSet<DriverNode>> getTokenRanges(
            final TableReference tableReference,
            final Node currentNode)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenewClusterWide(keyspace, currentNode);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> maybeRenewClusterWide(
            final String keyspace,
            final Node currentNode)
    {
        ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> replication = buildTokenMap(
                keyspace,
                true,
                currentNode);

        return CLUSTER_WIDE_KEYSPACE_REPLICATION_CACHE
                .compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    private ImmutableMap<LongTokenRange, ImmutableSet<DriverNode>> buildTokenMap(
            final String keyspace,
            final boolean clusterWide,
            final Node currentNode)
    {
        ImmutableMap.Builder<LongTokenRange, ImmutableSet<DriverNode>> replicationBuilder = ImmutableMap.builder();
        Map<Set<Node>, ImmutableSet<DriverNode>> replicaCache = new HashMap<>();
        Metadata metadata = mySession.getMetadata();
        Optional<TokenMap> tokenMap = metadata.getTokenMap();
        if (!tokenMap.isPresent())
        {
            throw new IllegalStateException("Cannot determine ranges, is metadata/tokenMap disabled?");
        }
        String keyspaceName = quoteIfNeeded(keyspace);
        Set<TokenRange> tokenRanges;
        if (clusterWide)
        {
            tokenRanges = tokenMap.get().getTokenRanges();
        }
        else
        {
            tokenRanges = tokenMap.get().getTokenRanges(keyspaceName, currentNode);
        }
        for (TokenRange tokenRange : tokenRanges)
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<DriverNode> replicas
                    = replicaCache.computeIfAbsent(tokenMap.get().getReplicas(keyspaceName, tokenRange), this::convert);

            replicationBuilder.put(longTokenRange, replicas);
        }

        return replicationBuilder.build();
    }

    private ImmutableSet<DriverNode> convert(final Set<Node> nodes)
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

    private LongTokenRange convert(final TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
