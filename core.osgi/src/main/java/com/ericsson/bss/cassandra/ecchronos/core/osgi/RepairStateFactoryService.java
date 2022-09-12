/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.PostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

@Component(service = RepairStateFactory.class)
public class RepairStateFactoryService implements RepairStateFactory
{
    @Reference(service = HostStates.class,
            cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile HostStates myHostStates;

    @Reference(service = TableRepairMetrics.class,
            cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile TableRepairMetrics myTableRepairMetrics;

    @Reference(service = RepairHistoryProvider.class,
            cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairHistoryProvider myRepairHistoryProvider;

    @Reference(service = ReplicationState.class,
            cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ReplicationState myReplicationState;

    private volatile RepairStateFactoryImpl myDelegateRepairStateFactory;

    @Activate
    public final void activate()
    {
        myDelegateRepairStateFactory = RepairStateFactoryImpl.builder()
                .withReplicationState(myReplicationState)
                .withHostStates(myHostStates)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();
    }

    @Override
    public final RepairState create(final TableReference tableReference,
                                    final RepairConfiguration repairConfiguration,
                                    final PostUpdateHook postUpdateHook)
    {
        return myDelegateRepairStateFactory.create(tableReference, repairConfiguration, postUpdateHook);
    }
}
