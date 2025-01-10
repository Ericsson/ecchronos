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

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static com.ericsson.bss.cassandra.ecchronos.core.utils.Metadata.quoteIfNeeded;

/**
 * Utility class to generate a token -&gt; replicas map for a specific table.
 */
public class ReplicationStateImpl implements ReplicationState
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationStateImpl.class);

    private static final Map<String, Map<LongTokenRange, Set<DriverNode>>>
            KEYSPACE_REPLICATION_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Map<LongTokenRange, Set<DriverNode>>>
            CLUSTER_WIDE_KEYSPACE_REPLICATION_CACHE = new ConcurrentHashMap<>();

    private final NodeResolver myNodeResolver;
    private final CqlSession mySession;
    private final Node myLocalNode;

    public ReplicationStateImpl(final NodeResolver nodeResolver, final CqlSession session, final Node localNode)
    {
        myNodeResolver = nodeResolver;
        mySession = session;
        myLocalNode = localNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<DriverNode> getNodes(final TableReference tableReference, final LongTokenRange tokenRange)
    {
        String keyspace = tableReference.getKeyspace();

        Map<LongTokenRange, Set<DriverNode>> replication = maybeRenew(keyspace);
        return getNodes(replication, tokenRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<DriverNode> getReplicas(final TableReference tableReference)
    {
        Map<LongTokenRange, Set<DriverNode>> tokens = getTokenRangeToReplicas(tableReference);
        Set<DriverNode> allReplicas = new HashSet<>();
        for (Set<DriverNode> replicas : tokens.values())
        {
            allReplicas.addAll(replicas);
        }
        return ImmutableSet.copyOf(allReplicas);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<DriverNode> getNodesClusterWide(final TableReference tableReference,
                                                        final LongTokenRange tokenRange)
    {
        String keyspace = tableReference.getKeyspace();

        Map<LongTokenRange, Set<DriverNode>> replication = maybeRenewClusterWide(keyspace);
        return getNodes(replication, tokenRange);
    }

    private Set<DriverNode> getNodes(final Map<LongTokenRange, Set<DriverNode>> replication,
                                              final LongTokenRange tokenRange)
    {
        Set<DriverNode> nodes = replication.get(tokenRange);

        if (nodes == null)
        {
            for (Map.Entry<LongTokenRange, Set<DriverNode>> entry : replication.entrySet())
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
    public Map<LongTokenRange, Set<DriverNode>> getTokenRangeToReplicas(final TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenew(keyspace);
    }

    private Map<LongTokenRange, Set<DriverNode>> maybeRenew(final String keyspace)
    {
        Map<LongTokenRange, Set<DriverNode>> replication = buildTokenMap(keyspace, false);

        return KEYSPACE_REPLICATION_CACHE.compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    /**
     * Get token ranges.
     *
     * @param tableReference Table reference.
     * @return Nodes and their ranges
     */
    @Override
    public Map<LongTokenRange, Set<DriverNode>> getTokenRanges(final TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();
        return maybeRenewClusterWide(keyspace);
    }

    private Map<LongTokenRange, Set<DriverNode>> maybeRenewClusterWide(final String keyspace)
    {
        Map<LongTokenRange, Set<DriverNode>> replication = buildTokenMap(keyspace, true);

        return CLUSTER_WIDE_KEYSPACE_REPLICATION_CACHE
                .compute(keyspace, (k, v) -> !replication.equals(v) ? replication : v);
    }

    private Map<LongTokenRange, Set<DriverNode>> buildTokenMap(final String keyspace,
                                                                                 final boolean clusterWide)
    {
        ImmutableMap.Builder<LongTokenRange, Set<DriverNode>> replicationBuilder = ImmutableMap.builder();
        Map<Set<Node>, Set<DriverNode>> replicaCache = new HashMap<>();
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
            tokenRanges = tokenMap.get().getTokenRanges(keyspaceName, myLocalNode);
        }
        for (TokenRange tokenRange : tokenRanges)
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            Set<DriverNode> replicas
                    = replicaCache.computeIfAbsent(tokenMap.get().getReplicas(keyspaceName, tokenRange), this::convert);

            replicationBuilder.put(longTokenRange, replicas);
        }

        return replicationBuilder.build();
    }

    private Set<DriverNode> convert(final Set<Node> nodes)
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
