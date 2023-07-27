/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.rest.osgi;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.rest.ScheduleRepairManagementREST;
import com.ericsson.bss.cassandra.ecchronos.rest.ScheduleRepairManagementRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * OSGi component wrapping {@link ScheduleRepairManagementREST} bound with OSGi services.
 */
@Component
public class RepairManagementScheduleRESTComponent implements ScheduleRepairManagementREST
{
    @Reference (service = RepairScheduler.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile RepairScheduler myRepairScheduler;

    private volatile ScheduleRepairManagementREST myDelegateScheduleRESTImpl;

    @Activate
    public final synchronized void activate()
    {
        myDelegateScheduleRESTImpl = new ScheduleRepairManagementRESTImpl(myRepairScheduler);
    }

    @Override
    public final ResponseEntity<List<Schedule>> getSchedules(final String keyspace, final String table)
    {
        return myDelegateScheduleRESTImpl.getSchedules(keyspace, table);
    }

    @Override
    public final ResponseEntity<Schedule> getSchedules(final String id, final boolean full)
    {
        return myDelegateScheduleRESTImpl.getSchedules(id, full);
    }
}
