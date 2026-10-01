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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents the synchronization state of a node as stored in Cassandra.
 *
 * @param ecchronosId the ecChronos instance identifier.
 * @param datacenterName the name of the datacenter the node belongs to.
 * @param nodeId the unique identifier of the node.
 * @param lastConnection the timestamp of the last connection.
 * @param nextConnection the timestamp of the next expected connection.
 * @param nodeEndpoint the endpoint address of the node.
 * @param nodeStatus the current status of the node.
 * @param stale whether the node's row has not been refreshed within the configured threshold.
 * @param lastHeartbeatAgeMs the time in milliseconds since the last successful heartbeat, or -1 if unknown.
 */
public record NodeSyncState(
    String ecchronosId,
    String datacenterName,
    UUID nodeId,
    Instant lastConnection,
    Instant nextConnection,
    String nodeEndpoint,
    String nodeStatus,
    boolean stale,
    long lastHeartbeatAgeMs
)
{
    /** Column name for the ecChronos instance identifier. */
    public static final String COLUMN_ECCHRONOS_ID = "ecchronos_id";
    /** Column name for the datacenter name. */
    public static final String COLUMN_DATACENTER_NAME = "datacenter_name";
    /** Column name for the node identifier. */
    public static final String COLUMN_NODE_ID = "node_id";
    /** Column name for the last connection timestamp. */
    public static final String COLUMN_LAST_CONNECTION = "last_connection";
    /** Column name for the next connection timestamp. */
    public static final String COLUMN_NEXT_CONNECTION = "next_connection";
    /** Column name for the node endpoint address. */
    public static final String COLUMN_NODE_ENDPOINT = "node_endpoint";
    /** Column name for the node status. */
    public static final String COLUMN_NODE_STATUS = "node_status";

    /**
     * Creates a {@link NodeSyncState} from a Cassandra row, without staleness evaluation
     * (i.e. {@code stale} is always {@code false}).
     *
     * @param row the Cassandra row to read from.
     * @return a new NodeSyncState instance.
     */
    public static NodeSyncState fromRow(final Row row)
    {
        return fromRow(row, 0L);
    }

    /**
     * Creates a {@link NodeSyncState} from a Cassandra row and evaluates staleness.
     *
     * <p>A node is considered stale when its row has not been refreshed within
     * {@code staleThresholdInMs}: that is, when {@code now - last_connection > staleThresholdInMs}.
     * With the heartbeat mechanism, a live instance refreshes {@code last_connection} every
     * interval, so a growing gap indicates the owning instance is no longer renewing the row
     * (e.g. a permanently decommissioned datacenter) even though {@code node_status} still reads
     * its last observed value. A threshold of {@code 0} or negative disables staleness.</p>
     *
     * @param row the Cassandra row to read from.
     * @param staleThresholdInMs the staleness threshold in milliseconds.
     * @return a new NodeSyncState instance.
     */
    public static NodeSyncState fromRow(final Row row, final long staleThresholdInMs)
    {
        Instant lastConnection = row.getInstant(COLUMN_LAST_CONNECTION);
        long lastHeartbeatAgeMs = ageInMs(lastConnection);
        boolean stale = staleThresholdInMs > 0 && lastConnection != null
                && lastHeartbeatAgeMs > staleThresholdInMs;
        return new NodeSyncState(
            row.getString(COLUMN_ECCHRONOS_ID),
            row.getString(COLUMN_DATACENTER_NAME),
            row.getUuid(COLUMN_NODE_ID),
            lastConnection,
            row.getInstant(COLUMN_NEXT_CONNECTION),
            row.getString(COLUMN_NODE_ENDPOINT),
            row.getString(COLUMN_NODE_STATUS),
            stale,
            lastHeartbeatAgeMs
        );
    }

    /**
     * Returns the age in milliseconds since the given last connection (the time since the last
     * successful heartbeat/write), or {@code -1} if it is unknown.
     *
     * @param lastConnection the last connection timestamp, may be null.
     * @return the age in milliseconds, or {@code -1} when unknown.
     */
    private static long ageInMs(final Instant lastConnection)
    {
        if (lastConnection == null)
        {
            return -1;
        }
        return Math.max(0, System.currentTimeMillis() - lastConnection.toEpochMilli());
    }
}
