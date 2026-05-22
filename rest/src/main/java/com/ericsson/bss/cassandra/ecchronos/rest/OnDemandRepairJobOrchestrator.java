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
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * Orchestrates on-demand repair job scheduling and querying.
 */
public final class OnDemandRepairJobOrchestrator
{
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;
    private final TableReferenceFactory myTableReferenceFactory;
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;
    private final OnDemandRepairRequestValidator myValidator;

    public OnDemandRepairJobOrchestrator(
            final OnDemandRepairScheduler onDemandRepairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final OnDemandRepairRequestValidator validator)
    {
        myOnDemandRepairScheduler = onDemandRepairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
        myValidator = validator;
    }

    public List<OnDemandRepair> getRepairs(final String keyspace, final String table, final String hostId)
    {
        Collection<Predicate<OnDemandRepairJobView>> filters = new ArrayList<>();
        if (keyspace != null)
        {
            filters.add(job -> keyspace.equals(job.getTableReference().getKeyspace()));
            if (table != null)
            {
                filters.add(job -> table.equals(job.getTableReference().getTable()));
            }
        }
        if (hostId != null)
        {
            UUID host = parseIdOrThrow(hostId);
            filters.add(job -> host.equals(job.getNodeId()));
        }
        Predicate<OnDemandRepairJobView> filter = filters.stream().reduce(Predicate::and).orElse(x -> true);
        return getClusterWideOnDemandJobs(filter);
    }

    public List<OnDemandRepair> getRepairs(final String nodeID, final String jobID)
    {
        boolean allNodes = "all".equalsIgnoreCase(nodeID);
        List<OnDemandRepair> repairJobs;
        if (allNodes)
        {
            Predicate<OnDemandRepairJobView> filter = jobID != null
                    ? job -> parseIdOrThrow(jobID).equals(job.getJobId())
                    : x -> true;
            repairJobs = getClusterWideOnDemandJobs(filter);
        }
        else
        {
            UUID myNodeID = parseIdOrThrow(nodeID);
            repairJobs = jobID != null
                    ? getOnDemandJobs(myNodeID).stream()
                        .filter(job -> parseIdOrThrow(jobID).equals(job.jobID)).toList()
                    : getOnDemandJobs(myNodeID);
        }
        if (jobID != null && repairJobs.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return repairJobs;
    }

    public List<OnDemandRepair> runRepair(
            final String nodeID,
            final String keyspace,
            final String table,
            final RepairType repairType,
            final boolean all,
            final boolean forceRepairTWCS,
            final boolean forceRepairDisabled)
    {
        try
        {
            myValidator.checkValidClusterRun(nodeID, all, keyspace, table);
            UUID nodeUUID = nodeID == null ? null : parseIdOrThrow(nodeID);
            myValidator.validateNodeExists(nodeUUID);

            if (keyspace != null)
            {
                return getOnDemandRepairsForKeyspace(keyspace, table, repairType, nodeUUID,
                        forceRepairTWCS, forceRepairDisabled);
            }
            return runLocalOrCluster(nodeUUID, repairType, myTableReferenceFactory.forCluster(),
                    forceRepairTWCS, forceRepairDisabled);
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private List<OnDemandRepair> getOnDemandRepairsForKeyspace(
            final String keyspace,
            final String table,
            final RepairType repairType,
            final UUID nodeUUID,
            final boolean forceRepairTWCS,
            final boolean forceRepairDisabled) throws EcChronosException
    {
        if (table != null)
        {
            TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
            if (tableReference == null)
            {
                throw new ResponseStatusException(NOT_FOUND,
                        "Table " + keyspace + "." + table + " does not exist");
            }
            if (myValidator.rejectForTWCS(tableReference, forceRepairTWCS))
            {
                throw new ResponseStatusException(BAD_REQUEST,
                        "Table " + keyspace + "." + table + " uses TWCS");
            }
            if (!myOnDemandRepairScheduler.checkTableEnabled(tableReference, forceRepairDisabled))
            {
                throw new ResponseStatusException(BAD_REQUEST,
                        "Table " + keyspace + "." + table + " is disabled");
            }
            return runLocalOrCluster(nodeUUID, repairType,
                    Collections.singleton(tableReference), forceRepairTWCS, forceRepairDisabled);
        }
        return runLocalOrCluster(nodeUUID, repairType,
                myTableReferenceFactory.forKeyspace(keyspace), forceRepairTWCS, forceRepairDisabled);
    }

    private List<OnDemandRepair> runLocalOrCluster(
            final UUID nodeID,
            final RepairType repairType,
            final Set<TableReference> tables,
            final boolean forceRepairTWCS,
            final boolean forceRepairDisabled) throws EcChronosException
    {
        Collection<Node> availableNodes;
        if (nodeID == null)
        {
            availableNodes = myDistributedNativeConnectionProvider.getNodes().values();
        }
        else
        {
            availableNodes = new ArrayList<>();
            availableNodes.add(myDistributedNativeConnectionProvider.getNodes().get(nodeID));
        }

        List<OnDemandRepair> onDemandRepairs = new ArrayList<>();
        for (TableReference tableReference : tables)
        {
            for (Node eachNode : availableNodes)
            {
                if (myValidator.isRepairableTable(forceRepairTWCS, forceRepairDisabled, tableReference, eachNode))
                {
                    onDemandRepairs.add(new OnDemandRepair(
                            myOnDemandRepairScheduler.scheduleJob(tableReference, repairType, eachNode.getHostId())));
                }
            }
        }
        return onDemandRepairs;
    }

    private List<OnDemandRepair> getClusterWideOnDemandJobs(final Predicate<OnDemandRepairJobView> filter)
    {
        return myOnDemandRepairScheduler.getAllClusterWideRepairJobs().stream()
                .filter(filter)
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }

    private List<OnDemandRepair> getOnDemandJobs(final UUID nodeID)
    {
        return myOnDemandRepairScheduler.getAllRepairJobs(nodeID).stream()
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }
}
