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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.RepairEvent;

/**
 * Abstraction over the component that receives schema-change and node-lifecycle events and applies them to
 * the repair scheduler, decoupling {@link DefaultRepairConfigurationProvider} and {@link NodeLifecycleHandler}
 * from the concrete delivery mechanism.
 * <p>
 * There are two implementations, selected by the global repair type:
 * <ul>
 *     <li>the legacy per-node {@code NodeWorkerManager} (one worker thread per node), and</li>
 *     <li>the consolidated {@code SchemaChangeCoordinator} (a single cluster-wide ordered executor) used by
 *     the {@code unified_*} path.</li>
 * </ul>
 */
public interface SchemaChangeHandler
{
    /**
     * Broadcast a schema-change (or setup/close) event to be applied across the managed nodes.
     *
     * @param event the event to broadcast.
     */
    void broadcastEvent(RepairEvent event);

    /**
     * Handle a node being added to the cluster.
     *
     * @param node the node being added.
     */
    void addNode(Node node);

    /**
     * Handle a node being removed from the cluster.
     *
     * @param node the node being removed.
     */
    void removeNode(Node node);

    /**
     * Shut down the handler and release its resources.
     */
    void shutdown();
}
