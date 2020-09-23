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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.gson.Gson;
import org.springframework.web.server.ResponseStatusException;

import static org.springframework.http.HttpStatus.NOT_FOUND;


/**
 * When updating the path it should also be updated in the OSGi component.
 */
@RestController
public class RepairManagementRESTImpl implements RepairManagementREST
{
    private static final String PROTOCOL_VERSION = "v1";
    private static final String ENDPOINT_PREFIX = "/repair-management/" + PROTOCOL_VERSION;

    private static final Gson GSON = new Gson();

    @Autowired
    private final RepairScheduler myRepairScheduler;

    @Autowired
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    public RepairManagementRESTImpl(RepairScheduler repairScheduler, OnDemandRepairScheduler onDemandRepairScheduler)
    {
        myRepairScheduler = repairScheduler;
        myOnDemandRepairScheduler = onDemandRepairScheduler;
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/status")
    public String status()
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> true);

        return GSON.toJson(repairJobs);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/status/keyspaces/{keyspace}")
    public String keyspaceStatus(@PathVariable String keyspace)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(
                job -> keyspace.equals(job.getTableReference().getKeyspace()));

        return GSON.toJson(repairJobs);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/status/keyspaces/{keyspace}/tables/{table}")
    public String tableStatus(@PathVariable String keyspace, @PathVariable String table)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(
                job -> new TableReference(keyspace, table).equals(job.getTableReference()));

        return GSON.toJson(repairJobs);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/status/keyspaces/{keyspace}/tables/{table}/ids/{id}")
    public String jobStatus(@PathVariable String keyspace, @PathVariable String table, @PathVariable String id)
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
    @GetMapping(ENDPOINT_PREFIX + "/config")
    public String config()
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(job -> true);

        return GSON.toJson(configurations);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/config/keyspaces/{keyspace}")
    public String keyspaceConfig(@PathVariable String keyspace)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(
                job -> job.getTableReference().getKeyspace().equals(keyspace));

        return GSON.toJson(configurations);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/config/keyspaces/{keyspace}/tables/{table}")
    public String tableConfig(@PathVariable String keyspace, @PathVariable String table)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(
                job -> job.getTableReference().equals(new TableReference(keyspace, table)));

        return GSON.toJson(configurations);
    }

    @Override
    @PostMapping(ENDPOINT_PREFIX + "/schedule/keyspaces/{keyspace}/tables/{table}")
    public String scheduleJob(@PathVariable String keyspace, @PathVariable String table)
    {
        RepairJobView repairJobView = null;
        try
        {
            repairJobView = myOnDemandRepairScheduler.scheduleJob(new TableReference(keyspace, table));
        } catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, "Not Found", e);
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
