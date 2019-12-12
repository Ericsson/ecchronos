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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairSchedulerREST;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairSchedulerRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import javax.ws.rs.Path;

/**
 * OSGi component wrapping {@link RepairSchedulerREST} bound with OSGi services.
 */
@Component
@Path("/repair-scheduler/v1")
public class RepairSchedulerRESTComponent implements RepairSchedulerREST
{
    @Reference (service = RepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairScheduler myRepairScheduler;

    private volatile RepairSchedulerRESTImpl myDelegateRESTImpl;

    @Activate
    public synchronized void activate()
    {
        myDelegateRESTImpl = new RepairSchedulerRESTImpl(myRepairScheduler);
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
    public String config()
    {
        return myDelegateRESTImpl.config();
    }
}
