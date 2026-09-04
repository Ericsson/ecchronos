/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.CloseEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.SetupEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

/**
 * Decouples schema-change handling from the per-node {@link com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker}
 * threads and the {@link RepairScheduler}.
 * <p>
 * There are two tracks:
 * <ul>
 *     <li><b>Per-node track</b> — driven by {@code NodeWorker} threads (one per managed node). It handles only
 *     <em>non-incremental</em> repair configurations, which are inherently per-node (vnode / parallel vnode).</li>
 *     <li><b>Global incremental track</b> — driven directly by the schema-change listener without going through the
 *     per-node worker threads. It handles only <em>incremental</em> repair configurations, of which there is a single
 *     logical job per table. It resolves the configuration once and hands it to the scheduler on any managed node; the
 *     {@link RepairScheduler} then deduplicates to one job per table and picks the deterministic coordinator.</li>
 * </ul>
 * A table is either incremental or non-incremental (never both), so the two tracks never both act on the same table.
 */
public class SchemaRefresher
{
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final RepairScheduler myRepairScheduler;
    private final TableReferenceFactory myTableReferenceFactory;
    private final Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
    private final CqlSession mySession;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    /**
     * Constructs a SchemaRefresher.
     *
     * @param replicatedTableProvider the provider for replicated table information.
     * @param repairScheduler the scheduler for managing repair configurations.
     * @param tableReferenceFactory the factory for creating table references.
     * @param repairConfigurationFunction the function providing repair configurations for a table.
     * @param session the CQL session for metadata access.
     * @param nativeConnectionProvider the provider used to pick a managed node for the global incremental track. May
     *         be {@code null}, in which case the incremental track is disabled.
     */
    public SchemaRefresher(
            final ReplicatedTableProvider replicatedTableProvider,
            final RepairScheduler repairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final Function<TableReference, Set<RepairConfiguration>> repairConfigurationFunction,
            final CqlSession session,
            final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        myReplicatedTableProvider = replicatedTableProvider;
        myRepairScheduler = repairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myRepairConfigurationFunction = repairConfigurationFunction;
        mySession = session;
        myNativeConnectionProvider = nativeConnectionProvider;
    }


    /**
     * Set up repair configurations for all tables in the keyspace on the given node.
     *
     * @param node the node to set up configurations for.
     * @param setupEvent the setup event containing the keyspace information.
     */
    public final void setupConfiguration(final Node node, final SetupEvent setupEvent)
    {
        String keyspaceName = setupEvent.keyspace().getName().asInternal();
        if (myReplicatedTableProvider.accept(node, keyspaceName))
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(node, tableReference, tableMetadata));
        }
    }

    /**
     * Deal with keyspace creation on the per-node track.
     *
     * @param node the node this per-node track belongs to.
     * @param keyspaceEvent the keyspace creation event to handle.
     */
    public void onKeyspaceCreated(final Node node, final KeyspaceCreatedEvent keyspaceEvent)
    {
        String keyspaceName = keyspaceEvent.keyspace().getName().asInternal();
        if (myReplicatedTableProvider.accept(node, keyspaceName))
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(node, tableReference, tableMetadata));
        }
        else
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> myRepairScheduler.removeConfiguration(node, tableReference));
        }
    }

    /**
     * Deal with table creation on the per-node track.
     *
     * @param node the node this per-node track belongs to.
     * @param tableEvent the table creation event to handle.
     */
    public void onTableCreated(final Node node, final TableCreatedEvent tableEvent)
    {
        if (myReplicatedTableProvider.accept(node, tableEvent.table().getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(tableEvent.table().getKeyspace().asInternal(),
                    tableEvent.table().getName().asInternal());
            updateConfiguration(node, tableReference, tableEvent.table());
        }
    }

    /**
     * Update repair configuration for a specific table on the given node.
     *
     * @param node the node to update configuration for.
     * @param tableReference the table reference.
     * @param table the table metadata.
     */
    public final void updateConfiguration(
            final Node node,
            final TableReference tableReference,
            final TableMetadata table)
    {
        Set<RepairConfiguration> repairConfigurations = myRepairConfigurationFunction.apply(tableReference);
        Set<RepairConfiguration> enabledRepairConfigurations = new HashSet<>();
        for (RepairConfiguration repairConfiguration: repairConfigurations)
        {
            if (!RepairConfiguration.DISABLED.equals(repairConfiguration)
                    && !isTableIgnored(table, repairConfiguration.getIgnoreTWCSTables()))
            {
                enabledRepairConfigurations.add(repairConfiguration);
            }
        }
        myRepairScheduler.putConfigurations(node, tableReference, enabledRepairConfigurations);
    }

    private boolean isTableIgnored(final TableMetadata table, final boolean ignore)
    {
        Map<CqlIdentifier, Object> tableOptions = table.getOptions();
        if (tableOptions == null)
        {
            return false;
        }
        Map<String, String> compaction
                = (Map<String, String>) tableOptions.get(CqlIdentifier.fromInternal("compaction"));
        if (compaction == null)
        {
            return false;
        }
        return ignore
                && "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy".equals(compaction.get("class"));
    }

    /**
     * Apply a consumer operation to all tables in the given keyspace.
     *
     * @param keyspaceName the keyspace to iterate.
     * @param consumer the operation to apply to each table.
     */
    public final void allTableOperation(
            final String keyspaceName,
            final BiConsumer<TableReference, TableMetadata> consumer)
    {
        for (TableMetadata tableMetadata : Metadata.getKeyspace(mySession, keyspaceName).get().getTables().values())
        {
            String tableName = tableMetadata.getName().asInternal();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference, tableMetadata);
        }
    }

    /**
     * Remove repair configurations for all tables in the keyspace on the given node.
     *
     * @param node the node to remove configurations for.
     * @param closeEvent the close event containing the keyspace information.
     */
    public final void close(final Node node, final CloseEvent closeEvent)
    {
        allTableOperation(closeEvent.keyspace().getName().asInternal(), (tableReference, tableMetadata) -> myRepairScheduler.removeConfiguration(node, tableReference));
    }

    /**
     * Deal with Table removal on the per-node track.
     *
     * @param node the node this per-node track belongs to.
     * @param table the table metadata for the table to remove configuration for.
     */
    public void removeConfiguration(final Node node, final TableMetadata table)
    {
        TableReference tableReference = myTableReferenceFactory.forTable(table);
        myRepairScheduler.removeConfiguration(node, tableReference);
    }

    /**
     * Remove all repair configurations for the given node.
     *
     * @param nodeId the node id to remove all configurations for.
     */
    public final void removeAllConfigurationsForNode(final UUID nodeId)
    {
        myRepairScheduler.removeAllConfigurationsForNode(nodeId);
    }

}
