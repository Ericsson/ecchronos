/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Path;

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.gson.Gson;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Path("/repair/demand/v1")
public class OnDemandRepairSchedulerRESTImpl implements OnDemandRepairSchedulerREST
{
    private static final Gson GSON = new Gson();

    private final OnDemandRepairScheduler myRepairScheduler;

    @Inject
    public OnDemandRepairSchedulerRESTImpl(OnDemandRepairScheduler repairScheduler)
    {
        myRepairScheduler = repairScheduler;
    }

    @Override
    public String get(String keyspace, String table)
    {
        Optional<RepairJobView> repairJobView = findRepairJob(new TableReference(keyspace, table));

        return repairJobView
                .map(CompleteRepairJob::new)
                .map(GSON::toJson)
                .orElse("{}");
    }

    @Override
    public String list()
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> true);

        return GSON.toJson(repairJobs);
    }

    @Override
    public String listKeyspace(String keyspace)
    {
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(
                job -> keyspace.equals(job.getTableReference().getKeyspace()));

        return GSON.toJson(repairJobs);
    }

    @Override
    public String scheduleJob(String keyspace, String table)
    {
        TableReference tableReference = new TableReference(keyspace, table);
        myRepairScheduler.scheduleJob(tableReference);

        return GSON.toJson("Success");
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
}
