/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.CloseEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.SetupEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableDroppedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class NodeWorker implements Runnable
{
    private final Node myNode;
    private final BlockingQueue<RepairEvent> myEventQueue = new LinkedBlockingQueue<>();
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final RepairScheduler myRepairScheduler;
    private final TableReferenceFactory myTableReferenceFactory;
    private final Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;
    private final CqlSession mySession;

    public NodeWorker(
            final Node node,
            final ReplicatedTableProvider replicatedTableProvider,
            final RepairScheduler repairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final Function<TableReference, Set<RepairConfiguration>> repairConfigurationFunction,
            final CqlSession session)
    {
        myNode = node;
        myReplicatedTableProvider = replicatedTableProvider;
        myRepairScheduler = repairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myRepairConfigurationFunction = repairConfigurationFunction;
        mySession = session;
    }

    public void submitEvent(final RepairEvent event)
    {
        myEventQueue.offer(event);
    }

    @Override
    public void run()
    {
        while (!Thread.currentThread().isInterrupted())
        {
            try
            {
                RepairEvent event = myEventQueue.take();
                handleEvent(event);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleEvent(final RepairEvent event)
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
            removeConfiguration(tableEvent.table());
        }
        else if (event instanceof SetupEvent setupEvent)
        {
            setupConfiguration(setupEvent);
        }
        else if (event instanceof CloseEvent closeEvent)
        {
            close(closeEvent);
        }
    }

    private void close(final CloseEvent closeEvent)
    {
        allTableOperation(closeEvent.keyspace().getName().asInternal(), (tableReference, tableMetadata) -> myRepairScheduler.removeConfiguration(myNode, tableReference));
    }

    private void setupConfiguration(final SetupEvent setupEvent)
    {
        String keyspaceName = setupEvent.keyspace().getName().asInternal();
        if (myReplicatedTableProvider.accept(myNode, keyspaceName))
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(myNode, tableReference, tableMetadata));
        }
    }

    private void onKeyspaceCreated(final KeyspaceCreatedEvent keyspaceEvent)
    {
        String keyspaceName = keyspaceEvent.keyspace().getName().asInternal();
        if (myReplicatedTableProvider.accept(myNode, keyspaceName))
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> updateConfiguration(myNode, tableReference, tableMetadata));
        }
        else
        {
            allTableOperation(keyspaceName, (tableReference, tableMetadata) -> myRepairScheduler.removeConfiguration(myNode, tableReference));
        }
    }

    private void onTableCreated(final TableCreatedEvent tableEvent)
    {
        if (myReplicatedTableProvider.accept(myNode, tableEvent.table().getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(tableEvent.table().getKeyspace().asInternal(),
                    tableEvent.table().getName().asInternal());
            updateConfiguration(myNode, tableReference, tableEvent.table());
        }
    }

    private void updateConfiguration(
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

    private void allTableOperation(
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

    private void removeConfiguration(final TableMetadata table)
    {
        TableReference tableReference = myTableReferenceFactory.forTable(table);
        myRepairScheduler.removeConfiguration(myNode, tableReference);
    }
}

