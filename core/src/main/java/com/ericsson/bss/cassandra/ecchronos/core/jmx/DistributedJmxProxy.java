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
package com.ericsson.bss.cassandra.ecchronos.core.jmx;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.util.UUID;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnector;

/**
 * Cassandra JMX proxy interface used to interact with a Cassandra node using JMX.
 */
public interface DistributedJmxProxy extends Closeable
{
    /**
     * Add a listener to the storage service interface.
     *
     * @param nodeID   The nodeID to get the JMXConnector
     * @param listener The listener to add.
     * @return
     * @see #removeStorageServiceListener(UUID, NotificationListener)
     */
    boolean addStorageServiceListener(UUID nodeID, NotificationListener listener);

    /**
     * Get a list of textual representations of IP addresses of the current live nodes.
     *
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @return A list of the live nodes.
     * @see #getUnreachableNodes(UUID)
     */
    List<String> getLiveNodes(UUID nodeID);

    /**
     * Get a list of textual representations of IP addresses of the current unreachable nodes.
     *
     * @return A list of the unreachable nodes.
     * @see #getLiveNodes(UUID)
     */
    List<String> getUnreachableNodes(UUID nodeID);

    /**
     * Perform a repair using the provided keyspace and options.
     *
     * @param keyspace
     *            The keyspace to repair.
     * @param options
     *            The options for the repair.
     * @return a positive value if a repair was started, zero otherwise.
     *
     * @see com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairOptions
     */
    int repairAsync(UUID nodeID, String keyspace, Map<String, String> options);

    /**
     * Force the termination of all repairs session on all the nodes.
     * <p>
     * This will not terminate repairs on other nodes but will affect other nodes running repair.
     */
    void forceTerminateAllRepairSessions();

    /**
     * Force the termination of all repair session on the specified node.
     * <p>
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * This will not terminate repairs on other nodes but will affect other nodes running repair.
     */
    void forceTerminateAllRepairSessionsInSpecificNode(UUID nodeID);
    /**
     * Remove a listener from the storage service interface.
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @param listener
     *            The listener to remove.
     *
     * @see #addStorageServiceListener(UUID, NotificationListener)
     */
    void removeStorageServiceListener(UUID nodeID, NotificationListener listener);

    /**
     * Get the live disk space used for the provided table.
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @param tableReference
     *            The table to get the live disk space for.
     * @return The live disk space used by the provided table.
     */
    long liveDiskSpaceUsed(UUID nodeID, TableReference tableReference);

    /**
     * Get max repaired at for the provided table.
     * Only usable when running incremental repairs.
     *
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @param tableReference The table to get max repaired at for.
     * @return Max repaired at or 0 if it cannot be determined.
     */
    long getMaxRepairedAt(UUID nodeID, TableReference tableReference);

    /**
     * Gets repaired ratio for a specific table.
     * Only usable when running incremental repairs.
     *
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @param tableReference The table to get repaired ratio for.
     * @return The repaired ratio or 0 if it cannot be determined.
     */
    double getPercentRepaired(UUID nodeID, TableReference tableReference);

    /**
     * Retrieves the current operational status of the local Cassandra node via JMX.
     * Returns a string indicating the node's state (e.g., "NORMAL", "JOINING", "LEAVING", "MOVING")
     * or "Unknown" if the status is undeterminable.
     *
     * @param nodeID
     *            The nodeID to get the JMXConnector
     * @return A string representing the node's status.
     */
    String getNodeStatus(UUID nodeID);

    /**
     * validate if the given JMXConnector is available.
     *
     * @param jmxConnector
     *            The jmxConnector to validate
     * @return A boolean representing the node's connection status.
     */
    boolean validateJmxConnection(JMXConnector jmxConnector);
}
