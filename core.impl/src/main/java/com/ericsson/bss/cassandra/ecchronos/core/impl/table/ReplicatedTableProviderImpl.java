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
import com.google.common.collect.ImmutableSet;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    private final CqlSession mySession;
    private final TableReferenceFactory myTableReferenceFactory;

    /**
     * Cached accepted-table snapshot. The accepted set only changes on schema/topology changes, so it is computed
     * lazily and reused until {@link #invalidate()} clears it (called from the driver schema/topology event path).
     * This stops the per-60s recompute and "not replicated" log flood on the hot path. See issue #1944.
     */
    private final AtomicReference<Set<TableReference>> myCachedTables = new AtomicReference<>();

    /**
     * Monotonic generation bumped by every {@link #invalidate()}. {@link #getAll()} captures it before recomputing
     * and only publishes the result if it has not changed since, so a schema/topology change landing during a
     * recompute is never masked by publishing the pre-change snapshot (the next call recomputes instead). See #1944.
     */
    private final AtomicLong myGeneration = new AtomicLong();

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
     * <p>
     * Returns a cached snapshot of the accepted tables. The snapshot is (re)computed lazily only after
     * {@link #invalidate()} is called on a schema/topology change, so a steady-state call does no node x keyspace
     * recomputation and emits no logging.
     */
    @Override
    public final Set<TableReference> getAll()
    {
        Set<TableReference> snapshot = myCachedTables.get();
        if (snapshot != null)
        {
            return snapshot;
        }

        // Capture the generation before reading the driver metadata so a concurrent invalidate() during the compute
        // can be detected. Only publish the result if no invalidate happened meanwhile; otherwise the snapshot may
        // reflect pre-change metadata and must not become the cached value (the next call recomputes). See #1944.
        long generation = myGeneration.get();
        Set<TableReference> computed = computeReplicatedTables();
        if (myGeneration.get() == generation)
        {
            // compareAndSet(null, ...) also avoids clobbering a snapshot another thread already published.
            myCachedTables.compareAndSet(null, computed);
        }
        return computed;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Drops the cached accepted-table snapshot and bumps the generation so a recompute already in flight does not
     * publish a now-stale snapshot; the next {@link #getAll()} recomputes. Called from the driver schema/topology
     * event path. See issue #1944.
     */
    @Override
    public final void invalidate()
    {
        myGeneration.incrementAndGet();
        myCachedTables.set(null);
    }

    /**
     * Recompute the accepted tables across all managed nodes, collecting (once per recompute) the keyspaces that were
     * not accepted because they are not replicated to a node's local data center, and logging them as a single
     * consolidated line rather than one WARN per keyspace per call. See issue #1944.
     *
     * @return an immutable snapshot of the accepted tables.
     */
    private Set<TableReference> computeReplicatedTables()
    {
        Set<String> notReplicated = new TreeSet<>();
        Set<TableReference> tables = myNativeConnectionProvider.getNodes().values().stream()
                .flatMap(node -> mySession.getMetadata().getKeyspaces().values().stream()
                        .filter(k -> accept(node, k.getName().asInternal(), notReplicated))
                        .flatMap(k -> k.getTables().values().stream())
                        .map(myTableReferenceFactory::forTable)
                )
                .collect(Collectors.toSet());

        if (!notReplicated.isEmpty())
        {
            LOG.info("Ignoring {} keyspace(s) not replicated to the local data center: {}",
                    notReplicated.size(), notReplicated);
        }
        return ImmutableSet.copyOf(tables);
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
        return accept(node, keyspace, null);
    }

    /**
     * Acceptance check with optional collection of keyspaces skipped because they are not replicated to the node's
     * local data center. When {@code notReplicated} is non-null, such keyspaces are added to it (for a single
     * consolidated log line) instead of being logged per-keyspace here. See issue #1944.
     *
     * @param node the node to check the keyspace against.
     * @param keyspace the keyspace to check.
     * @param notReplicated optional sink for not-replicated keyspace names, or {@code null} to log per-keyspace.
     * @return true if the keyspace should be repaired.
     */
    private boolean accept(final Node node, final String keyspace, final Set<String> notReplicated)
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

            switch (replicationClass)
            {
            case SIMPLE_STRATEGY:
                return validateSimpleStrategy(replication);
            case NETWORK_TOPOLOGY_STRATEGY:
                return validateNetworkTopologyStrategy(node, keyspace, replication, notReplicated);
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
            String> replication,
            final Set<String> notReplicated)
    {
        String localDc = currentNode.getDatacenter();

        if (localDc == null)
        {
            LOG.error("Local data center is not defined, ignoring keyspace {}", keyspace);
            return false;
        }

        if (!replication.containsKey(localDc))
        {
            if (notReplicated != null)
            {
                // Collect for a single consolidated log line per recompute instead of a per-keyspace WARN flood.
                notReplicated.add(keyspace);
            }
            else
            {
                LOG.debug("Keyspace {} not replicated by node, ignoring.", keyspace);
            }
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


