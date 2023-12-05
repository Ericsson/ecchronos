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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import javax.management.NotificationListener;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Cassandra JMX proxy interface used to interact with the local Cassandra node using JMX.
 */
public interface JmxProxy extends Closeable
{
    /**
     * Add a listener to the storage service interface.
     *
     * @param listener
     *            The listener to add.
     * @see #removeStorageServiceListener(NotificationListener)
     */
    void addStorageServiceListener(NotificationListener listener);

    /**
     * Get a list of textual representations of IP addresses of the current live nodes.
     *
     * @return A list of the live nodes.
     * @see #getUnreachableNodes()
     */
    List<String> getLiveNodes();

    /**
     * Get a list of textual representations of IP addresses of the current unreachable nodes.
     *
     * @return A list of the unreachable nodes.
     * @see #getLiveNodes()
     */
    List<String> getUnreachableNodes();

    /**
     * Perform a repair using the provided keyspace and options.
     *
     * @param keyspace
     *            The keyspace to repair.
     * @param options
     *            The options for the repair.
     * @return a positive value if a repair was started, zero otherwise.
     *
     * @see com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions
     */
    int repairAsync(String keyspace, Map<String, String> options);

    /**
     * Force the termination of all repair session on the local node.
     * <p>
     * This will not terminate repairs on other nodes but will affect other nodes running repair.
     */
    void forceTerminateAllRepairSessions();

    /**
     * Remove a listener from the storage service interface.
     *
     * @param listener
     *            The listener to remove.
     *
     * @see #addStorageServiceListener(NotificationListener)
     */
    void removeStorageServiceListener(NotificationListener listener);

    /**
     * Get the live disk space used for the provided table.
     *
     * @param tableReference
     *            The table to get the live disk space for.
     * @return The live disk space used by the provided table.
     */
    long liveDiskSpaceUsed(TableReference tableReference);

    /**
     * Get max repaired at for the provided table.
     * Only usable when running incremental repairs.
     *
     * @param tableReference The table to get max repaired at for.
     * @return Max repaired at or 0 if it cannot be determined.
     */
    long getMaxRepairedAt(TableReference tableReference);

    /**
     * Gets repaired ratio for a specific table.
     * Only usable when running incremental repairs.
     *
     * @param tableReference The table to get repaired ratio for.
     * @return The repaired ratio or 0 if it cannot be determined.
     */
    double getPercentRepaired(TableReference tableReference);

    String getNodeStatus() ;
}
