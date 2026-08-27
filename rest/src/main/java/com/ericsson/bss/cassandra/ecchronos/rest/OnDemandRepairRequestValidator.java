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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

/**
 * Validates on-demand repair requests.
 */
public final class OnDemandRepairRequestValidator
{
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;

    /**
     * Constructs a new validator for on-demand repair requests.
     *
     * @param onDemandRepairScheduler the scheduler used for on-demand repairs.
     * @param replicatedTableProvider the provider used to check table replication.
     * @param distributedNativeConnectionProvider the provider for distributed native connections.
     */
    public OnDemandRepairRequestValidator(
            final OnDemandRepairScheduler onDemandRepairScheduler,
            final ReplicatedTableProvider replicatedTableProvider,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider)
    {
        myOnDemandRepairScheduler = onDemandRepairScheduler;
        myReplicatedTableProvider = replicatedTableProvider;
        myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
    }

    /**
     * Validates parameters for a cluster-wide repair run. Throws a BAD_REQUEST exception
     * if no node is specified and the "all" flag is not set, or if a table is provided without a keyspace.
     *
     * @param nodeID the target node identifier, may be {@code null}.
     * @param all whether all nodes should be targeted.
     * @param keyspace the keyspace name, may be {@code null}.
     * @param table the table name, may be {@code null}.
     */
    public void checkValidClusterRun(final String nodeID, final boolean all, final String keyspace, final String table)
    {
        if (nodeID == null && !all)
        {
            throw new ResponseStatusException(BAD_REQUEST, "If a node is not specified then parameter all should be true");
        }
        if (keyspace == null && table != null)
        {
            throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
        }
    }

    /**
     * Validates that the specified node UUID exists in the cluster. Throws a BAD_REQUEST exception
     * if the node is not found or not managed by the local instance.
     *
     * @param nodeUUID the UUID of the node to validate, may be {@code null}.
     */
    public void validateNodeExists(final UUID nodeUUID)
    {
        if (nodeUUID != null && myDistributedNativeConnectionProvider.getNodes().get(nodeUUID) == null)
        {
            throw new ResponseStatusException(BAD_REQUEST,
                    "Node specified is not a valid node or is not managed by the local instance");
        }
    }

    /**
     * Determines whether a table should be rejected for repair because it uses TWCS
     * (TimeWindowCompactionStrategy) and TWCS repair is not forced.
     *
     * @param tableReference the table reference to check.
     * @param forceRepairTWCS whether repair of TWCS tables is forced.
     * @return {@code true} if the table should be rejected, {@code false} otherwise.
     */
    public boolean rejectForTWCS(final TableReference tableReference, final boolean forceRepairTWCS)
    {
        return !forceRepairTWCS && tableReference.getTwcs()
                && myOnDemandRepairScheduler.getRepairConfiguration().getIgnoreTWCSTables();
    }

    /**
     * Checks whether a table is eligible for repair based on TWCS policy, replication, and enabled status.
     *
     * @param forceRepairTWCS whether repair of TWCS tables is forced.
     * @param forceRepairDisabled whether repair of disabled tables is forced.
     * @param tableReference the table reference to check.
     * @param node the node to validate replication against.
     * @return {@code true} if the table is eligible for repair, {@code false} otherwise.
     */
    public boolean isRepairableTable(final boolean forceRepairTWCS, final boolean forceRepairDisabled,
            final TableReference tableReference, final Node node)
    {
        return !rejectForTWCS(tableReference, forceRepairTWCS)
                && myReplicatedTableProvider.accept(node, tableReference.getKeyspace())
                && myOnDemandRepairScheduler.checkTableEnabled(tableReference, forceRepairDisabled);
    }

    /**
     * Returns the given repair type, or defaults to {@link RepairType#VNODE} if {@code null}.
     *
     * @param repairType the repair type to use, may be {@code null}.
     * @return the provided repair type or VNODE as default.
     */
    public RepairType getRepairTypeOrDefault(final RepairType repairType)
    {
        return repairType == null ? RepairType.VNODE : repairType;
    }
}
