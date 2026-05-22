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

    public OnDemandRepairRequestValidator(
            final OnDemandRepairScheduler onDemandRepairScheduler,
            final ReplicatedTableProvider replicatedTableProvider,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider)
    {
        myOnDemandRepairScheduler = onDemandRepairScheduler;
        myReplicatedTableProvider = replicatedTableProvider;
        myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
    }

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

    public void validateNodeExists(final UUID nodeUUID)
    {
        if (nodeUUID != null && myDistributedNativeConnectionProvider.getNodes().get(nodeUUID) == null)
        {
            throw new ResponseStatusException(BAD_REQUEST,
                    "Node specified is not a valid node or is not managed by the local instance");
        }
    }

    public boolean rejectForTWCS(final TableReference tableReference, final boolean forceRepairTWCS)
    {
        return !forceRepairTWCS && tableReference.getTwcs()
                && myOnDemandRepairScheduler.getRepairConfiguration().getIgnoreTWCSTables();
    }

    public boolean isRepairableTable(final boolean forceRepairTWCS, final boolean forceRepairDisabled,
            final TableReference tableReference, final Node node)
    {
        return !rejectForTWCS(tableReference, forceRepairTWCS)
                && myReplicatedTableProvider.accept(node, tableReference.getKeyspace())
                && myOnDemandRepairScheduler.checkTableEnabled(tableReference, forceRepairDisabled);
    }

    public RepairType getRepairTypeOrDefault(final RepairType repairType)
    {
        return repairType == null ? RepairType.VNODE : repairType;
    }
}
