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

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
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

import java.time.Duration;

/**
 * OSGi component wrapping {@link RepairManagementREST} bound with OSGi services.
 */
@Component
public class RepairManagementRESTComponent implements RepairManagementREST
{
    @Reference(service = TableReferenceFactory.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile TableReferenceFactory myTableReferenceFactory;

    @Reference(service = ReplicatedTableProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile ReplicatedTableProvider myReplicatedTableProvider;

    @Reference(service = RepairStatsProvider.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile RepairStatsProvider myRepairStatsProvider;

    private volatile RepairManagementRESTImpl myDelegateRESTImpl;

    @Activate
    public final synchronized void activate()
    {
        myDelegateRESTImpl = new RepairManagementRESTImpl(myTableReferenceFactory,
                myReplicatedTableProvider, myRepairStatsProvider);
    }

    @Override
    public final ResponseEntity<RepairInfo> getRepairInfo(final String keyspace,
                                                          final String table,
                                                          final Long since,
                                                          final Duration duration,
                                                          final boolean isLocal)
    {
        return myDelegateRESTImpl.getRepairInfo(keyspace, table, since, duration, isLocal);
    }
}
