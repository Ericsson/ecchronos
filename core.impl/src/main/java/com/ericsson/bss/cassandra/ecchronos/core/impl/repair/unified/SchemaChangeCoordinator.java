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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaChangeHandler;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaRefresher;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.CloseEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.SetupEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableDroppedEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cluster-wide handler for schema-change and setup events on the {@code unified_*} path.
 * <p>
 * Instead of the legacy per-node {@code NodeWorker} threads (one thread per managed node consuming a per-node
 * event queue), a single ordered executor processes each schema event once and applies it to every managed
 * node via the {@link SchemaRefresher}. The {@code SchemaRefresher} still gates per node on whether the node
 * replicates the keyspace, so only replicating nodes report configurations to the (unified) scheduler; the
 * scheduler then consolidates those per-node reports into a single multi-node job per table.
 * <p>
 * Events are handled on a single-thread executor to preserve ordering (a table-created event is not processed
 * before the keyspace-created event that preceded it).
 */
public final class SchemaChangeCoordinator implements SchemaChangeHandler, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeCoordinator.class);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final SchemaRefresher mySchemaRefresher;
    private final ExecutorService myExecutor;

    /**
     * Constructs a SchemaChangeCoordinator.
     *
     * @param nativeConnectionProvider the provider of managed nodes.
     * @param schemaRefresher the schema refresher applied per managed node.
     */
    public SchemaChangeCoordinator(
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final SchemaRefresher schemaRefresher)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        mySchemaRefresher = schemaRefresher;
        myExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("SchemaChangeCoordinator-%d").build());
    }

    /**
     * Dispatch a broadcast event to every managed node, preserving event ordering.
     *
     * @param event the event to apply.
     */
    @Override
    public void broadcastEvent(final RepairEvent event)
    {
        if (event instanceof KeyspaceCreatedEvent keyspaceEvent)
        {
            onKeyspaceCreated(keyspaceEvent);
        }
        else if (event instanceof TableCreatedEvent tableEvent)
        {
            onTableCreated(tableEvent);
        }
        else if (event instanceof TableDroppedEvent tableEvent)
        {
            onTableDropped(tableEvent);
        }
        else if (event instanceof SetupEvent setupEvent)
        {
            setup(setupEvent);
        }
        else if (event instanceof CloseEvent closeEvent)
        {
            onClose(closeEvent);
        }
        else
        {
            LOG.warn("Unhandled schema event type: {}", event);
        }
    }

    /**
     * Handle a node being added: set up all of its replicated keyspaces/tables.
     *
     * @param node the node being added.
     */
    @Override
    public void addNode(final Node node)
    {
        setupNewNode(node);
    }

    /**
     * Handle a node being removed: deschedule all of its configurations.
     *
     * @param node the node being removed.
     */
    @Override
    public void removeNode(final Node node)
    {
        submit(() -> mySchemaRefresher.removeAllConfigurationsForNode(node.getHostId()));
    }

    /**
     * Shut down the coordinator.
     */
    @Override
    public void shutdown()
    {
        close();
    }

    private Collection<Node> managedNodes()
    {
        return new ArrayList<>(myNativeConnectionProvider.getNodes().values());
    }

    /**
     * Handle a keyspace-created (or updated) event for every managed node.
     *
     * @param event the keyspace event.
     */
    public void onKeyspaceCreated(final KeyspaceCreatedEvent event)
    {
        submit(() -> managedNodes().forEach(node -> mySchemaRefresher.onKeyspaceCreated(node, event)));
    }

    /**
     * Handle a table-created (or updated) event for every managed node.
     *
     * @param event the table event.
     */
    public void onTableCreated(final TableCreatedEvent event)
    {
        submit(() -> managedNodes().forEach(node -> mySchemaRefresher.onTableCreated(node, event)));
    }

    /**
     * Handle a table-dropped event for every managed node.
     *
     * @param event the table dropped event.
     */
    public void onTableDropped(final TableDroppedEvent event)
    {
        submit(() -> managedNodes().forEach(node -> mySchemaRefresher.removeConfiguration(node, event.table())));
    }

    /**
     * Handle a keyspace close event for every managed node.
     *
     * @param event the close event.
     */
    public void onClose(final CloseEvent event)
    {
        submit(() -> managedNodes().forEach(node -> mySchemaRefresher.close(node, event)));
    }

    /**
     * Run initial setup for a keyspace across every managed node.
     *
     * @param event the setup event.
     */
    public void setup(final SetupEvent event)
    {
        submit(() -> managedNodes().forEach(node -> mySchemaRefresher.setupConfiguration(node, event)));
    }

    /**
     * Run setup for all keyspaces on a single (typically newly added) node, so its replicated tables are
     * scheduled. Keyspace metadata is read from the cluster session.
     *
     * @param node the node to set up.
     */
    public void setupNewNode(final Node node)
    {
        submit(() ->
        {
            for (KeyspaceMetadata keyspace
                    : myNativeConnectionProvider.getCqlSession().getMetadata().getKeyspaces().values())
            {
                mySchemaRefresher.setupConfiguration(node, new SetupEvent(keyspace));
            }
        });
    }

    private void submit(final Runnable task)
    {
        try
        {
            myExecutor.execute(() ->
            {
                try
                {
                    task.run();
                }
                catch (Exception e)
                {
                    LOG.error("Error handling schema change event", e);
                }
            });
        }
        catch (RejectedExecutionException e)
        {
            LOG.debug("Schema change coordinator shutting down, ignoring event", e);
        }
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                myExecutor.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            myExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
