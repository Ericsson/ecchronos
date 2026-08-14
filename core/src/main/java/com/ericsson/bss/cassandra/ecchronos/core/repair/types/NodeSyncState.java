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
 */
public record NodeSyncState(
    String ecchronosId,
    String datacenterName,
    UUID nodeId,
    Instant lastConnection,
    Instant nextConnection,
    String nodeEndpoint,
    String nodeStatus
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
     * Creates a {@link NodeSyncState} from a Cassandra row.
     *
     * @param row the Cassandra row to read from.
     * @return a new NodeSyncState instance.
     */
    public static NodeSyncState fromRow(final Row row)
    {
        return new NodeSyncState(
            row.getString(COLUMN_ECCHRONOS_ID),
            row.getString(COLUMN_DATACENTER_NAME),
            row.getUuid(COLUMN_NODE_ID),
            row.getInstant(COLUMN_LAST_CONNECTION),
            row.getInstant(COLUMN_NEXT_CONNECTION),
            row.getString(COLUMN_NODE_ENDPOINT),
            row.getString(COLUMN_NODE_STATUS)
        );
    }
}
