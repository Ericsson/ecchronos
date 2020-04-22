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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.gson.Gson;

import javax.inject.Inject;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Path("/repair-management/v1")
public class RepairManagementRESTImpl implements RepairManagementREST
{
    private static final Gson GSON = new Gson();

    private final RepairScheduler myRepairScheduler;

    @Inject
    public RepairManagementRESTImpl(RepairScheduler repairScheduler)
    {
        myRepairScheduler = repairScheduler;
    }

    @Override
    public String scheduledStatus()
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> true);

        return GSON.toJson(repairJobs);
    }

    @Override
    public String scheduledKeyspaceStatus(String keyspace)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> keyspace.equals(job.getTableReference().getKeyspace()));

        return GSON.toJson(repairJobs);
    }

    @Override
    public String scheduledTableStatus(String keyspace, String table)
    {
        Optional<RepairJobView> repairJobView = findRepairJob(new TableReference(keyspace, table));

        return repairJobView
                .map(CompleteRepairJob::new)
                .map(GSON::toJson)
                .orElse("{}");
    }

    @Override
    public String scheduledConfig()
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(job -> true);

        return GSON.toJson(configurations);
    }

    @Override
    public String scheduledKeyspaceConfig(String keyspace)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(job -> job.getTableReference().getKeyspace().equals(keyspace));

        return GSON.toJson(configurations);
    }

    @Override
    public String scheduledTableConfig(String keyspace, String table)
    {
        Optional<RepairJobView> repairJobView = findRepairJob(new TableReference(keyspace, table));

        return repairJobView
                .map(TableRepairConfig::new)
                .map(GSON::toJson)
                .orElse("{}");
    }

    private List<ScheduledRepairJob> getScheduledRepairJobs(Predicate<RepairJobView> filter)
    {
        List<RepairJobView> repairJobViewList = myRepairScheduler.getCurrentRepairJobs();

        return repairJobViewList.stream()
                .filter(filter)
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());
    }

    private Optional<RepairJobView> findRepairJob(TableReference tableReference)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(job -> job.getTableReference().equals(tableReference))
                .findFirst();
    }

    private List<TableRepairConfig> getTableRepairConfigs(Predicate<RepairJobView> filter)
    {
        return myRepairScheduler.getCurrentRepairJobs()
                .stream()
                .filter(filter)
                .map(TableRepairConfig::new)
                .collect(Collectors.toList());
    }
}
