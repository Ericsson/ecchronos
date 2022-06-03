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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@RestController
public class RepairManagementRESTImpl implements RepairManagementREST //NOPMD Possible god class, will be much smaller once v1 is removed
{
    private static final String ROOT = "/repair-management/";
    private static final String DEPRECATED_PROTOCOL_VERSION = "v1";
    private static final String PROTOCOL_VERSION = "v2";
    private static final String ENDPOINT_PREFIX = ROOT + DEPRECATED_PROTOCOL_VERSION;
    private static final String ENDPOINT_PREFIX_V2 = ROOT + PROTOCOL_VERSION;

    @Autowired
    private final RepairScheduler myRepairScheduler;

    @Autowired
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Autowired
    private final TableReferenceFactory myTableReferenceFactory;

    public RepairManagementRESTImpl(RepairScheduler repairScheduler, OnDemandRepairScheduler demandRepairScheduler,
            TableReferenceFactory tableReferenceFactory)
    {
        myRepairScheduler = repairScheduler;
        myOnDemandRepairScheduler = demandRepairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX_V2 + "/schedules")
    public ResponseEntity<List<Schedule>> getSchedules(@RequestParam(required = false) String keyspace,
                                                       @RequestParam(required = false) String table)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                List<Schedule> repairJobs = getScheduledRepairJobs(forTable(keyspace, table));
                return ResponseEntity.ok(repairJobs);
            }
            List<Schedule> repairJobs = getScheduledRepairJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace()));
            return ResponseEntity.ok(repairJobs);
        }
        else if (table == null)
        {
            List<Schedule> repairJobs = getScheduledRepairJobs(job -> true);
            return ResponseEntity.ok(repairJobs);
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX_V2 + "/schedules/{id}")
    public ResponseEntity<Schedule> getSchedules(@PathVariable String id, @RequestParam(required = false) boolean full)
    {

        UUID uuid;
        try
        {
            uuid = UUID.fromString(id);
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
        Optional<RepairJobView> repairJobView = getScheduleView(uuid);
        if (!repairJobView.isPresent())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(new Schedule(repairJobView.get(), full));

    }


    @Override
    @GetMapping(ENDPOINT_PREFIX_V2 + "/repairs")
    public ResponseEntity<List<OnDemandRepair>> getRepairs(@RequestParam(required = false) String keyspace,
                                                           @RequestParam(required = false) String table,
                                                           @RequestParam(required = false) String hostId)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                if (hostId == null)
                {
                    List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(forTable(keyspace, table));
                    return ResponseEntity.ok(repairJobs);
                }
                UUID host = parseIdOrThrow(hostId);
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> keyspace.equals(job.getTableReference().getKeyspace()) &&
                                                                                    table.equals(job.getTableReference().getTable()) &&
                                                                                    host.equals(job.getHostId()));
                return ResponseEntity.ok(repairJobs);
            }
            if (hostId == null)
            {
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(
                        job -> keyspace.equals(job.getTableReference().getKeyspace()));
                return ResponseEntity.ok(repairJobs);
            }
            UUID host = parseIdOrThrow(hostId);
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace()) &&
                           host.equals(job.getHostId()));
            return ResponseEntity.ok(repairJobs);
        }
        else if (table == null)
        {
            if (hostId == null)
            {
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> true);
                return ResponseEntity.ok(repairJobs);
            }
            UUID host = parseIdOrThrow(hostId);
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> host.equals(job.getHostId()));
            return ResponseEntity.ok(repairJobs);
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    @Override
    @GetMapping(ENDPOINT_PREFIX_V2 + "/repairs/{id}")
    public ResponseEntity<List<OnDemandRepair>> getRepairs(@PathVariable String id,
                                                           @RequestParam(required = false) String hostId)
    {
        UUID uuid = parseIdOrThrow(id);
        if (hostId == null)
        {
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> uuid.equals(job.getId()));
            if (repairJobs.isEmpty())
            {
                throw new ResponseStatusException(NOT_FOUND);
            }
            return ResponseEntity.ok(repairJobs);
        }
        UUID host = parseIdOrThrow(hostId);
        List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> uuid.equals(job.getId()) && host.equals(job.getHostId()));
        if (repairJobs.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(repairJobs);
    }

    private UUID parseIdOrThrow(String id)
    {
        try
        {
            UUID uuid = UUID.fromString(id);
            return uuid;
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
    }


    @Override
    @PostMapping(ENDPOINT_PREFIX_V2 + "/repairs")
    public ResponseEntity<List<OnDemandRepair>> triggerRepair(@RequestParam String keyspace,
                                                              @RequestParam String table,
                                                              @RequestParam(required = false) boolean isLocal)
    {
        try
        {
            if (isLocal)
            {
                RepairJobView repairJobView = myOnDemandRepairScheduler.scheduleJob(
                        myTableReferenceFactory.forTable(keyspace, table));
                return ResponseEntity.ok(Collections.singletonList(new OnDemandRepair(repairJobView)));
            }
            List<RepairJobView> repairJobView = myOnDemandRepairScheduler.scheduleClusterWideJob(
                    myTableReferenceFactory.forTable(keyspace, table));
            return ResponseEntity.ok(repairJobView.stream().map(OnDemandRepair::new).collect(Collectors.toList()));
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/status")
    public ResponseEntity<List<ScheduledRepairJob>> status()
    {
        List<ScheduledRepairJob> repairJobs = getAllRepairJobs(job -> true);
        return ResponseEntity.ok(repairJobs);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/status/keyspaces/{keyspace}")
    public ResponseEntity<List<ScheduledRepairJob>> keyspaceStatus(@PathVariable String keyspace)
    {
        List<ScheduledRepairJob> repairJobs = getAllRepairJobs(
                job -> keyspace.equals(job.getTableReference().getKeyspace()));
        return ResponseEntity.ok(repairJobs);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/status/keyspaces/{keyspace}/tables/{table}")
    public ResponseEntity<List<ScheduledRepairJob>> tableStatus(@PathVariable String keyspace,
            @PathVariable String table)
    {
        List<ScheduledRepairJob> repairJobs = getAllRepairJobs(forTable(keyspace, table));
        return ResponseEntity.ok(repairJobs);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/status/ids/{id}")
    public ResponseEntity<CompleteRepairJob> jobStatus(@PathVariable String id)
    {
        UUID uuid;
        try
        {
            uuid = UUID.fromString(id);
        }
        catch (IllegalArgumentException e)
        {
            //BAD REQUEST makes most sense here (UUID cannot be parsed)
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
        Optional<RepairJobView> repairJobView = getCompleteRepairJob(uuid);
        if (!repairJobView.isPresent())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(new CompleteRepairJob(repairJobView.get()));
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/config")
    public ResponseEntity<List<TableRepairConfig>> config()
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(job -> true);
        return ResponseEntity.ok(configurations);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/config/keyspaces/{keyspace}")
    public ResponseEntity<List<TableRepairConfig>> keyspaceConfig(@PathVariable String keyspace)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(
                job -> job.getTableReference().getKeyspace().equals(keyspace));
        return ResponseEntity.ok(configurations);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/config/keyspaces/{keyspace}/tables/{table}")
    public ResponseEntity<List<TableRepairConfig>> tableConfig(@PathVariable String keyspace,
            @PathVariable String table)
    {
        List<TableRepairConfig> configurations = getTableRepairConfigs(forTable(keyspace, table));
        return ResponseEntity.ok(configurations);
    }

    @Override
    @Deprecated
    @GetMapping(ENDPOINT_PREFIX + "/config/ids/{id}")
    public ResponseEntity<TableRepairConfig> jobConfig(@PathVariable String id)
    {
        UUID uuid;
        try
        {
            uuid = UUID.fromString(id);
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
        Optional<RepairJobView> repairJobView = getCompleteRepairJob(uuid);
        if (!repairJobView.isPresent())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(new TableRepairConfig(repairJobView.get()));
    }

    @Override
    @Deprecated
    @PostMapping(ENDPOINT_PREFIX + "/schedule/keyspaces/{keyspace}/tables/{table}")
    public ResponseEntity<ScheduledRepairJob> scheduleJob(@PathVariable String keyspace, @PathVariable String table)
    {
        try
        {
            RepairJobView repairJobView = myOnDemandRepairScheduler.scheduleJob(
                    myTableReferenceFactory.forTable(keyspace, table));
            return ResponseEntity.ok(new ScheduledRepairJob(repairJobView));
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private List<ScheduledRepairJob> getAllRepairJobs(Predicate<RepairJobView> filter)
    {
        return Stream
                .concat(myRepairScheduler.getCurrentRepairJobs().stream(),
                        myOnDemandRepairScheduler.getAllRepairJobs().stream())
                .filter(filter)
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());
    }

    private List<Schedule> getScheduledRepairJobs(Predicate<RepairJobView> filter)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(filter)
                .map(Schedule::new)
                .collect(Collectors.toList());
    }

    private Optional<RepairJobView> getCompleteRepairJob(UUID id)
    {
        return Stream
                .concat(myRepairScheduler.getCurrentRepairJobs().stream(),
                        myOnDemandRepairScheduler.getAllRepairJobs().stream())
                .filter(job -> job.getId().equals(id)).findFirst();
    }

    private List<OnDemandRepair> getClusterWideOnDemandJobs(Predicate<RepairJobView> filter)
    {
        return myOnDemandRepairScheduler.getAllClusterWideRepairJobs().stream()
                .filter(filter)
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }

    private Optional<RepairJobView> getScheduleView(UUID id)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(job -> job.getId().equals(id)).findFirst();
    }

    private List<TableRepairConfig> getTableRepairConfigs(Predicate<RepairJobView> filter)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(filter)
                .map(TableRepairConfig::new)
                .collect(Collectors.toList());
    }

    private Predicate<RepairJobView> forTable(String keyspace, String table)
    {
        return tableView ->
        {
            TableReference tableReference = tableView.getTableReference();
            return tableReference.getKeyspace().equals(keyspace)
                    && tableReference.getTable().equals(table);
        };
    }
}
