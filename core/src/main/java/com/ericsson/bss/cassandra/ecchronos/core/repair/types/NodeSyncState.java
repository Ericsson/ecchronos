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

public class NodeSyncState
{
    public static final String COLUMN_ECCHRONOS_ID = "ecchronos_id";
    public static final String COLUMN_DATACENTER_NAME = "datacenter_name";
    public static final String COLUMN_NODE_ID = "node_id";
    public static final String COLUMN_LAST_CONNECTION = "last_connection";
    public static final String COLUMN_NEXT_CONNECTION = "next_connection";
    public static final String COLUMN_NODE_ENDPOINT = "node_endpoint";
    public static final String COLUMN_NODE_STATUS = "node_status";

    private final String ecchronosId;
    private final String datacenterName;
    private final UUID nodeId;
    private final Instant lastConnection;
    private final Instant nextConnection;
    private final String nodeEndpoint;
    private final String nodeStatus;

    public NodeSyncState(final Row row)
    {
        ecchronosId = row.getString(COLUMN_ECCHRONOS_ID);
        datacenterName = row.getString(COLUMN_DATACENTER_NAME);
        nodeId = row.getUuid(COLUMN_NODE_ID);
        lastConnection = row.getInstant(COLUMN_LAST_CONNECTION);
        nextConnection = row.getInstant(COLUMN_NEXT_CONNECTION);
        nodeEndpoint = row.getString(COLUMN_NODE_ENDPOINT);
        nodeStatus = row.getString(COLUMN_NODE_STATUS);
    }

    public final String getEcchronosId()
    {
        return ecchronosId;
    }

    public final String getDatacenterName()
    {
        return datacenterName;
    }

    public final UUID getNodeId()
    {
        return nodeId;
    }

    public final Instant getLastConnection()
    {
        return lastConnection;
    }

    public final Instant getNextConnection()
    {
        return nextConnection;
    }

    public final String getNodeEndpoint()
    {
        return nodeEndpoint;
    }

    public final String getNodeStatus()
    {
        return nodeStatus;
    }
}
