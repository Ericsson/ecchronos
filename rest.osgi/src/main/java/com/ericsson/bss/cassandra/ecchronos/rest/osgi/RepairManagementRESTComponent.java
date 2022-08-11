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
package com.ericsson.bss.cassandra.ecchronos.rest.osgi;

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementREST;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * OSGi component wrapping {@link RepairManagementREST} bound with OSGi services.
 */
@Component
public class RepairManagementRESTComponent implements RepairManagementREST
{
    @Reference (service = RepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairScheduler myRepairScheduler;

    @Reference (service = OnDemandRepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Reference(service = TableReferenceFactory.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile TableReferenceFactory myTableReferenceFactory;

    @Reference(service = ReplicatedTableProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ReplicatedTableProvider myReplicatedTableProvider;

    private volatile RepairManagementRESTImpl myDelegateRESTImpl;

    @Activate
    public synchronized void activate()
    {
        myDelegateRESTImpl = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler,
                myTableReferenceFactory, myReplicatedTableProvider);
    }

    @Override
    public ResponseEntity<List<OnDemandRepair>> getRepairs(String keyspace, String table, String hostId)
    {
        return myDelegateRESTImpl.getRepairs(keyspace, table, hostId);
    }

    @Override
    public ResponseEntity<List<OnDemandRepair>> getRepairs(String id, String hostId)
    {
        return myDelegateRESTImpl.getRepairs(id, hostId);
    }

    @Override
    public ResponseEntity<List<Schedule>> getSchedules(String keyspace, String table)
    {
        return myDelegateRESTImpl.getSchedules(keyspace, table);
    }

    @Override
    public ResponseEntity<Schedule> getSchedules(String id, boolean full)
    {
        return myDelegateRESTImpl.getSchedules(id, full);
    }

    @Override
    public ResponseEntity<List<OnDemandRepair>> triggerRepair(String keyspace, String table, boolean isLocal)
    {
        return myDelegateRESTImpl.triggerRepair(keyspace, table, isLocal);
    }
}
