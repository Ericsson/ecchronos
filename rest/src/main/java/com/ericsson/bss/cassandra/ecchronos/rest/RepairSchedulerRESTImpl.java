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
@Path("/repair-scheduler/v1")
public class RepairSchedulerRESTImpl implements RepairSchedulerREST
{
    private static final Gson GSON = new Gson();

    private final RepairScheduler myRepairScheduler;

    @Inject
    public RepairSchedulerRESTImpl(RepairScheduler repairScheduler)
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
        List<ScheduledRepairJob> repairJobs = getScheduledRepairJobs(job -> keyspace.equals(job.getTableReference().getKeyspace()));

        return GSON.toJson(repairJobs);
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
