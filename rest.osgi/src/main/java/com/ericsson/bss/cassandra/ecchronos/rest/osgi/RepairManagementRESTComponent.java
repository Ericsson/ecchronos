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
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementREST;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import javax.ws.rs.Path;

/**
 * OSGi component wrapping {@link RepairManagementREST} bound with OSGi services.
 */
@Component
@Path("/repair-management/v1")
public class RepairManagementRESTComponent implements RepairManagementREST
{
    @Reference(service = RepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairScheduler myRepairScheduler;

    private volatile RepairManagementRESTImpl myDelegateRESTImpl;

    @Activate
    public synchronized void activate()
    {
        myDelegateRESTImpl = new RepairManagementRESTImpl(myRepairScheduler);
    }

    @Override
    public String scheduledStatus()
    {
        return myDelegateRESTImpl.scheduledStatus();
    }

    @Override
    public String scheduledKeyspaceStatus(String keyspace)
    {
        return myDelegateRESTImpl.scheduledKeyspaceStatus(keyspace);
    }

    @Override
    public String scheduledTableStatus(String keyspace, String table)
    {
        return myDelegateRESTImpl.scheduledTableStatus(keyspace, table);
    }

    @Override
    public String scheduledConfig()
    {
        return myDelegateRESTImpl.scheduledConfig();
    }

    @Override
    public String scheduledKeyspaceConfig(String keyspace)
    {
        return myDelegateRESTImpl.scheduledKeyspaceConfig(keyspace);
    }

    @Override
    public String scheduledTableConfig(String keyspace, String table)
    {
        return myDelegateRESTImpl.scheduledTableConfig(keyspace, table);
    }
}
