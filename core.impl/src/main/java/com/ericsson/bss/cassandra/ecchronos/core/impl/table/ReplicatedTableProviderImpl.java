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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Implementation of ReplicatedTableProvider for retrieving tables replicated by the local node.
 * The purpose of this is to abstract away java-driver related mocking from other components
 * trying to retrieve the tables that should be repaired.
 */
public class ReplicatedTableProviderImpl implements ReplicatedTableProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedTableProviderImpl.class);

    private static final String STRATEGY_CLASS = "class";
    private static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    private static final String SIMPLE_STRATEGY_REPLICATION_FACTOR = "replication_factor";

    private static final String SYSTEM_AUTH_KEYSPACE = "system_auth";

    private final CqlSession mySession;
    private final TableReferenceFactory myTableReferenceFactory;

    /**
     * Constructs a ReplicatedTableProviderImpl to manage table references in a replicated environment.
     *
     * @param session the {@link CqlSession} used to connect to the Cassandra cluster. Must not be {@code null}.
     * @param tableReferenceFactory the factory to create {@link TableReference} instances. Must not be {@code null}.
     * @param nativeConnectionProvider the {@link DistributedNativeConnectionProvider} object that contains the nodes in the Cassandra cluster. Must not be {@code null}.
     */
    public ReplicatedTableProviderImpl(
            final CqlSession session,
            final TableReferenceFactory tableReferenceFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        mySession = session;
        myTableReferenceFactory = tableReferenceFactory;
        myNativeConnectionProvider = nativeConnectionProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final Set<TableReference> getAll()
    {
        return myNativeConnectionProvider.getNodes().values().stream()
                .flatMap(node -> mySession.getMetadata().getKeyspaces().values().stream()
                        .filter(k -> accept(node, k.getName().asInternal()))
                        .flatMap(k -> k.getTables().values().stream())
                        .map(tb -> myTableReferenceFactory.forTable(tb.getKeyspace().asInternal(), tb.getName().asInternal()))
                )
                .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(
            final Node node,
            final String keyspace
    )
    {
        if (keyspace.startsWith("system") && !SYSTEM_AUTH_KEYSPACE.equals(keyspace))
        {
            return false;
        }

        Optional<KeyspaceMetadata> keyspaceMetadata = Metadata.getKeyspace(mySession, keyspace);

        if (keyspaceMetadata.isPresent())
        {
            Map<String, String> replication = keyspaceMetadata.get().getReplication();
            String replicationClass = replication.get(STRATEGY_CLASS);

            switch (replicationClass)
            {
            case SIMPLE_STRATEGY:
                return validateSimpleStrategy(replication);
            case NETWORK_TOPOLOGY_STRATEGY:
                return validateNetworkTopologyStrategy(node, keyspace, replication);
            default:
                LOG.warn("Replication strategy of type {} is not supported", replicationClass);
                break;
            }
        }

        return false;
    }

    private boolean validateSimpleStrategy(final Map<String, String> replication)
    {
        int replicationFactor = Integer.parseInt(replication.get(SIMPLE_STRATEGY_REPLICATION_FACTOR));

        return replicationFactor > 1;
    }

    private boolean validateNetworkTopologyStrategy(
            final Node currentNode,
            final String keyspace, final Map<String,
            String> replication)
    {
        String localDc = currentNode.getDatacenter();

        if (localDc == null)
        {
            LOG.error("Local data center is not defined, ignoring keyspace {}", keyspace);
            return false;
        }

        if (!replication.containsKey(localDc))
        {
            LOG.warn("Keyspace {} not replicated by node, ignoring.", keyspace);
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


