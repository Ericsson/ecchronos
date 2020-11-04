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
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementREST;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import org.osgi.service.component.annotations.*;

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

    private volatile RepairManagementRESTImpl myDelegateRESTImpl;

    @Activate
    public synchronized void activate()
    {
        myDelegateRESTImpl = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler,
                myTableReferenceFactory);
    }

    @Override
    public String status()
    {
        return myDelegateRESTImpl.status();
    }

    @Override
    public String keyspaceStatus(String keyspace)
    {
        return myDelegateRESTImpl.keyspaceStatus(keyspace);
    }

    @Override
    public String tableStatus(String keyspace, String table)
    {
        return myDelegateRESTImpl.tableStatus(keyspace, table);
    }

    @Override
    public String jobStatus(String id)
    {
        return myDelegateRESTImpl.jobStatus(id);
    }

    @Override
    public String config()
    {
        return myDelegateRESTImpl.config();
    }

    @Override
    public String keyspaceConfig(String keyspace)
    {
        return myDelegateRESTImpl.keyspaceConfig(keyspace);
    }

    @Override
    public String tableConfig(String keyspace, String table)
    {
        return myDelegateRESTImpl.tableConfig(keyspace, table);
    }

    @Override
    public String scheduleJob(String keyspace, String table)
    {
        return myDelegateRESTImpl.scheduleJob(keyspace, table);
    }
}
