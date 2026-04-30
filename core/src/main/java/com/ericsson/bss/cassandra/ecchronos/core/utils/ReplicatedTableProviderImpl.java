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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ReplicatedTableProviderImpl implements ReplicatedTableProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedTableProviderImpl.class);

    private static final String STRATEGY_CLASS = "class";
    private static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    private static final String SIMPLE_STRATEGY_REPLICATION_FACTOR = "replication_factor";

    private static final String SYSTEM_KEYSPACE_NAME = "system";
    private static final String SCHEMA_KEYSPACE_NAME = "system_schema";
    private static final String METADATA_KEYSPACE_NAME = "system_cluster_metadata";

    private static final String TRACE_KEYSPACE_NAME = "system_traces";
    private static final String ACCORD_KEYSPACE_NAME = "system_accord";
    private static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";

    private static final String VIRTUAL_SCHEMA = "system_virtual_schema";
    private static final String VIRTUAL_VIEWS = "system_views";
    private static final String VIRTUAL_METRICS = "system_metrics";
    private static final String VIRTUAL_ACCORD_DEBUG = "system_accord_debug";
    private static final String VIRTUAL_ACCORD_DEBUG_REMOTE = "system_accord_debug_remote";
    private static final Set<String> IGNORED_SYSTEM_KEYSPACES = ImmutableSet.of(
            SYSTEM_KEYSPACE_NAME,
            SCHEMA_KEYSPACE_NAME,
            METADATA_KEYSPACE_NAME,
            TRACE_KEYSPACE_NAME,
            ACCORD_KEYSPACE_NAME,
            DISTRIBUTED_KEYSPACE_NAME,
            VIRTUAL_SCHEMA,
            VIRTUAL_VIEWS,
            VIRTUAL_METRICS,
            VIRTUAL_ACCORD_DEBUG,
            VIRTUAL_ACCORD_DEBUG_REMOTE);

    private final Node myLocalNode;
    private final CqlSession mySession;
    private final TableReferenceFactory myTableReferenceFactory;

    public ReplicatedTableProviderImpl(final Node node,
                                       final CqlSession session,
                                       final TableReferenceFactory tableReferenceFactory)
    {
        myLocalNode = Objects.requireNonNull(node, "Local node cannot be null");
        mySession = Objects.requireNonNull(session, "CQL session cannot be null");
        myTableReferenceFactory = Objects.requireNonNull(tableReferenceFactory,
                "Table reference factory cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Set<TableReference> getAll()
    {
        return mySession.getMetadata().getKeyspaces().values().stream()
                .filter(k -> accept(k.getName().asInternal()))
                .flatMap(k -> k.getTables().values().stream())
                .map(tb -> myTableReferenceFactory.forTable(tb.getKeyspace().asInternal(), tb.getName().asInternal()))
                .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(final String keyspace)
    {
        if (IGNORED_SYSTEM_KEYSPACES.contains(keyspace))
        {
            return false;
        }

        Optional<KeyspaceMetadata> keyspaceMetadata = Metadata.getKeyspace(mySession, keyspace);

        if (keyspaceMetadata.isPresent())
        {
            Map<String, String> replication = keyspaceMetadata.get().getReplication();
            String replicationClass = replication.get(STRATEGY_CLASS);

            return switch (replicationClass)
            {
                case SIMPLE_STRATEGY -> validateSimpleStrategy(replication);
                case NETWORK_TOPOLOGY_STRATEGY -> validateNetworkTopologyStrategy(keyspace, replication);
                default ->
                {
                    LOG.warn("Replication strategy of type {} is not supported", replicationClass);
                    yield false;
                }
            };
        }

        return false;
    }

    private boolean validateSimpleStrategy(final Map<String, String> replication)
    {
        int replicationFactor = Integer.parseInt(replication.get(SIMPLE_STRATEGY_REPLICATION_FACTOR));

        return replicationFactor > 1;
    }

    private boolean validateNetworkTopologyStrategy(final String keyspace, final Map<String, String> replication)
    {
        String localDc = myLocalNode.getDatacenter();

        if (localDc == null)
        {
            LOG.error("Local data center is not defined, ignoring keyspace {}", keyspace);
            return false;
        }

        if (!replication.containsKey(localDc))
        {
            LOG.debug("Keyspace {} not replicated by local node, ignoring.", keyspace);
            return false;
        }

        return definedReplicationInNetworkTopologyStrategy(replication) > 1;
    }

    private int definedReplicationInNetworkTopologyStrategy(final Map<String, String> replication)
    {
        int replicationFactor = 0;

        for (Map.Entry<String, String> replicationEntry : replication.entrySet())
        {
            if (!STRATEGY_CLASS.equals(replicationEntry.getKey()))
            {
                replicationFactor += Integer.parseInt(replicationEntry.getValue());
            }
        }

        return replicationFactor;
    }
}
