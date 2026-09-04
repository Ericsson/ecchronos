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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.SchemaRefresher;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.CloseEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.SetupEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableDroppedEvent;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A worker that processes repair events for a specific Cassandra node.
 * Runs in its own thread, consuming events from a queue and applying
 * configuration changes or scheduling repairs accordingly.
 */
public class NodeWorker implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeWorker.class);
    private final Node myNode;
    private final SchemaRefresher mySchemaRefresher;
    private final BlockingQueue<RepairEvent> myEventQueue = new LinkedBlockingQueue<>();


    /**
     * Constructs a NodeWorker for the specified node.
     *
     * @param node the Cassandra node this worker handles.
     */
    public NodeWorker(final Node node, final SchemaRefresher schemaRefresher)
    {
        myNode = node;
        mySchemaRefresher = schemaRefresher;
    }

    /**
     * Submits a repair event to this worker's event queue for processing.
     *
     * @param event the repair event to submit.
     */
    public final void submitEvent(final RepairEvent event)
    {
        myEventQueue.offer(event);
    }

    @Override
    public final void run()
    {
        while (!Thread.currentThread().isInterrupted())
        {
            try
            {
                RepairEvent event = myEventQueue.take();
                LOG.debug("Handling event {}", event.toString());
                handleEvent(event);
            }
            catch (InterruptedException e)
            {
                LOG.debug("Nodeworker Thread interrupted ");
                Thread.currentThread().interrupt();
            }
            catch (Exception e)
            {
                LOG.error("Exception caught in main run look of NodeWorker {}", e);
            }
        }
    }

    private void handleEvent(final RepairEvent event)
    {
        if (event instanceof KeyspaceCreatedEvent keyspaceEvent)
        {
            mySchemaRefresher.onKeyspaceCreated(myNode, keyspaceEvent);
        }
        else if (event instanceof TableCreatedEvent tableEvent)
        {
            mySchemaRefresher.onTableCreated(myNode, tableEvent);
        }
        else if (event instanceof TableDroppedEvent tableEvent)
        {
            mySchemaRefresher.removeConfiguration(myNode, tableEvent.table());
        }
        else if (event instanceof SetupEvent setupEvent)
        {
            mySchemaRefresher.setupConfiguration(myNode, setupEvent);
        }
        else if (event instanceof CloseEvent closeEvent)
        {
            mySchemaRefresher.close(myNode, closeEvent);
        }
    }

    /**
     * Used for testing only.
     *
     * @return the current size of the event queue.
     */
    @VisibleForTesting
    public int getQueueSize()
    {
        return myEventQueue.size();
    }
}

