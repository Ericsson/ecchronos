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
package com.ericsson.bss.cassandra.ecchronos.rest.osgi;

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairSchedulerREST;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairSchedulerRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import javax.ws.rs.Path;

/**
 * OSGi component wrapping {@link OnDemandRepairSchedulerREST} bound with OSGi services.
 */
@Component
@Path("/repair/demand/v1")
public class OnDemandRepairSchedulerRESTComponent implements OnDemandRepairSchedulerREST
{
    @Reference (service = RepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile OnDemandRepairScheduler myRepairScheduler;

    private volatile OnDemandRepairSchedulerRESTImpl myDelegateRESTImpl;

    @Activate
    public synchronized void activate()
    {
        myDelegateRESTImpl = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);
    }

    @Override
    public String get(String keyspace, String table)
    {
        return myDelegateRESTImpl.get(keyspace, table);
    }

    @Override
    public String list()
    {
        return myDelegateRESTImpl.list();
    }

    @Override
    public String listKeyspace(String keyspace)
    {
        return myDelegateRESTImpl.listKeyspace(keyspace);
    }

    @Override
    public String scheduleJob(String keyspace, String table)
    {
        return myDelegateRESTImpl.scheduleJob(keyspace, table);
    }
}