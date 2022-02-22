/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduleView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Repair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduleWithVirutalNodes;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@SuppressWarnings("CPD-START")
public class RepairManagementRESTImplV2 implements RepairManagementRESTV2
{
    private static final String PROTOCOL_VERSION = "v2";
    private static final String ENDPOINT_PREFIX = "/repair-management/" + PROTOCOL_VERSION;

    private static final Gson GSON = new Gson();

    @Autowired
    private final RepairScheduler myRepairScheduler;

    @Autowired
    private final TableReferenceFactory myTableReferenceFactory;

    public RepairManagementRESTImplV2(RepairScheduler repairScheduler, TableReferenceFactory tableReferenceFactory)
    {
        myRepairScheduler = repairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/schedules")
    public String schedules()
    {
        List<Schedule> schedules = getSchedules(job -> true);
        return GSON.toJson(schedules);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/repairs")
    public String repairs()
    {
        List<Repair> repairs = myRepairScheduler.getRepairs().stream().map(Repair::new).collect(Collectors.toList());
        return GSON.toJson(repairs);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/schedules/keyspaces/{keyspace}")
    public String keyspaceSchedules(@PathVariable String keyspace)
    {
        List<Schedule> schedules = getSchedules(job -> keyspace.equals(job.getTableReference().getKeyspace()));
        return GSON.toJson(schedules);
    }

    private List<Schedule> getSchedules(Predicate<ScheduleView> filter)
    {
        return myRepairScheduler.getSchedules().stream()
                .filter(filter)
                .map(Schedule::new)
                .collect(Collectors.toList());
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/schedules/keyspaces/{keyspace}/tables/{table}")
    public String tableSchedules(@PathVariable String keyspace, @PathVariable String table)
    {
        List<Schedule> schedules = getSchedules(job -> keyspace.equals(job.getTableReference().getKeyspace()) &&
                table.equals(job.getTableReference().getTable()));
        return GSON.toJson(schedules);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/schedules/ids/{id}")
    public String scheduleStatus(@PathVariable String id)
    {
        try
        {
            Optional<ScheduleView> scheduleView = getCompleteSchedule(UUID.fromString(id));
            return scheduleView
                    .map(ScheduleWithVirutalNodes::new)
                    .map(GSON::toJson)
                    .orElse("{}");
        }
        catch(IllegalArgumentException e)
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
        List<TableRepairConfig> configurations = getTableRepairConfigs(forTable(keyspace, table));
        return GSON.toJson(configurations);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX + "/config/ids/{id}")
    public String jobConfig(@PathVariable String id)
    {
        try
        {
            Optional<ScheduleView> scheduleView = getCompleteSchedule(UUID.fromString(id));
            return scheduleView
                    .map(TableRepairConfig::new)
                    .map(GSON::toJson)
                    .orElse("{}");
        }
        catch(IllegalArgumentException e)
        {
            return "{}";
        }
    }

    @Override
    @PostMapping(ENDPOINT_PREFIX + "/repair/keyspaces/{keyspace}/tables/{table}")
    public String triggerRepair(@PathVariable String keyspace, @PathVariable String table)
    {
        OngoingRepair ongoingRepair = myRepairScheduler.scheduleOnDemandRepair(myTableReferenceFactory.forTable(keyspace, table));
        if (ongoingRepair == null)
        {
            throw new ResponseStatusException(NOT_FOUND, "Not Found");
        }
        return GSON.toJson(new Repair(ongoingRepair));
    }

    private Optional<ScheduleView> getCompleteSchedule(UUID id)
    {
        return myRepairScheduler.getSchedules().stream()
                .filter(job -> job.getId().equals(id)).findFirst();
    }

    private List<TableRepairConfig> getTableRepairConfigs(Predicate<ScheduleView> filter)
    {
        return myRepairScheduler.getSchedules().stream()
                .filter(filter)
                .map(TableRepairConfig::new)
                .collect(Collectors.toList());
    }

    private Predicate<ScheduleView> forTable(String keyspace, String table)
    {
        return tableView ->
        {
            TableReference tableReference = tableView.getTableReference();
            return tableReference.getKeyspace().equals(keyspace)
                    && tableReference.getTable().equals(table);
        };
    }
}
