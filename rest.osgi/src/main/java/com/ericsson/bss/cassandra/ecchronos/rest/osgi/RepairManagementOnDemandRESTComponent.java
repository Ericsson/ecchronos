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

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairManagementREST;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairManagementRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * OSGi component wrapping {@link OnDemandRepairManagementREST} bound with OSGi services.
 */
@Component
public class RepairManagementOnDemandRESTComponent implements OnDemandRepairManagementREST
{
    @Reference (service = OnDemandRepairScheduler.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Reference(service = TableReferenceFactory.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile TableReferenceFactory myTableReferenceFactory;

    @Reference(service = ReplicatedTableProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile ReplicatedTableProvider myReplicatedTableProvider;

    private volatile OnDemandRepairManagementREST myDelegateOnDemandRESTImpl;

    @Activate
    public final synchronized void activate()
    {
        myDelegateOnDemandRESTImpl = new OnDemandRepairManagementRESTImpl(myOnDemandRepairScheduler,
                myTableReferenceFactory, myReplicatedTableProvider);
    }

    @Override
    public final ResponseEntity<List<OnDemandRepair>> getRepairs(final String keyspace,
            final String table,
            final String hostId)
    {
        return myDelegateOnDemandRESTImpl.getRepairs(keyspace, table, hostId);
    }

    @Override
    public final ResponseEntity<List<OnDemandRepair>> getRepairs(final String id, final String hostId)
    {
        return myDelegateOnDemandRESTImpl.getRepairs(id, hostId);
    }

    @Override
    public final ResponseEntity<List<OnDemandRepair>> triggerRepair(final String keyspace,
            final String table,
            final boolean isLocal)
    {
        return myDelegateOnDemandRESTImpl.triggerRepair(keyspace, table, isLocal);
    }
}
