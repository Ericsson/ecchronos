/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.gson.Gson;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Path("/repair-management/v1")
public class RepairManagementRESTImpl implements RepairManagementREST
{
    private static final Gson GSON = new Gson();

    private final RepairScheduler myRepairScheduler;
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Inject
    public RepairManagementRESTImpl(RepairScheduler repairScheduler, OnDemandRepairScheduler demandRepairScheduler)
    {
        myRepairScheduler = repairScheduler;
        myOnDemandRepairScheduler = demandRepairScheduler;
    }

    @Override
    public String status()
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> true);

        return GSON.toJson(repairJobs);
    }

    @Override
    public String keyspaceStatus(String keyspace)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(
                job -> keyspace.equals(job.getTableReference().getKeyspace()));

        return GSON.toJson(repairJobs);
    }

    @Override
    public String tableStatus(String keyspace, String table)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(
                job -> new TableReference(keyspace, table).equals(job.getTableReference()));

        return GSON.toJson(repairJobs);
    }

    @Override
    public String jobStatus(String keyspace, String table, String id)
    {
        try
        {
            Optional<RepairJobView>  repairJobView = getCompleteRepairJob(new TableReference(keyspace, table), UUID.fromString(id));
            return repairJobView
                    .map(CompleteRepairJob::new)
                    .map(GSON::toJson)
                    .orElse("{}");
        } catch (IllegalArgumentException e)
        {
            return "{}";
        }
    }

    @Override
    public String config()
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(job -> true);

        return GSON.toJson(configurations);
    }

    @Override
    public String keyspaceConfig(String keyspace)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(
                job -> job.getTableReference().getKeyspace().equals(keyspace));

        return GSON.toJson(configurations);
    }

    @Override
    public String tableConfig(String keyspace, String table)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(
                job -> job.getTableReference().equals(new TableReference(keyspace, table)));

        return GSON.toJson(configurations);
    }

    @Override
    public String scheduleJob(String keyspace, String table)
    {
        RepairJobView repairJobView = null;
        try
        {
            repairJobView = myOnDemandRepairScheduler.scheduleJob(new TableReference(keyspace, table));
        } catch (EcChronosException e)
        {
            throw new NotFoundException(e);
        }
        return GSON.toJson(new ScheduledRepairJob(repairJobView));
    }

    private List<ScheduledRepairJob> getScheduledRepairJobs(Predicate<RepairJobView> filter)
    {
        return Stream
                .concat(myRepairScheduler.getCurrentRepairJobs().stream(),
                        myOnDemandRepairScheduler.getCurrentRepairJobs().stream())
                .filter(filter)
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());
    }

    private Optional<RepairJobView> getCompleteRepairJob(TableReference tableReference, UUID id)
    {
        Predicate<RepairJobView> matchesTable = job -> job.getTableReference().equals(tableReference);
        Predicate<RepairJobView> matchesId = job -> job.getId().equals(id);
        return Stream
                .concat(myRepairScheduler.getCurrentRepairJobs().stream(),
                        myOnDemandRepairScheduler.getCurrentRepairJobs().stream())
                .filter(matchesTable.and(matchesId))
                .findFirst();
    }

    private List<TableRepairConfig> getTableRepairConfigs(Predicate<RepairJobView> filter)
    {
        return Stream
                .concat(myRepairScheduler.getCurrentRepairJobs().stream(),
                        myOnDemandRepairScheduler.getCurrentRepairJobs().stream())
                .filter(filter)
                .map(TableRepairConfig::new)
                .collect(Collectors.toList());
    }
}
