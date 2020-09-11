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

import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@Component(service = RepairStateFactory.class)
@Designate(ocd = RepairStateFactoryService.Configuration.class)
public class RepairStateFactoryService implements RepairStateFactory
{
    private static final long DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS = 30L * 24L * 60L * 60L;

    @Reference(service = StatementDecorator.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile StatementDecorator myStatementDecorator;

    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider myNativeConnectionProvider;

    @Reference(service = HostStates.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile HostStates myHostStates;

    @Reference(service = TableRepairMetrics.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile TableRepairMetrics myTableRepairMetrics;

    @Reference(service = ReplicationState.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ReplicationState myReplicationState;

    private volatile RepairStateFactoryImpl myDelegateRepairStateFactory;

    @Activate
    public void activate(Configuration configuration)
    {
        long lookbackTimeInMillis = configuration.lookbackTimeSeconds() * 1000;
        RepairHistoryProvider repairHistoryProvider = new RepairHistoryProviderImpl(
                myNativeConnectionProvider.getSession(), myStatementDecorator, lookbackTimeInMillis);

        myDelegateRepairStateFactory = RepairStateFactoryImpl.builder()
                .withReplicationState(myReplicationState)
                .withHostStates(myHostStates)
                .withRepairHistoryProvider(repairHistoryProvider)
                .withTableRepairMetrics(myTableRepairMetrics)
                .build();
    }

    @Override
    public RepairState create(TableReference tableReference, RepairConfiguration repairConfiguration,
            PostUpdateHook postUpdateHook)
    {
        return myDelegateRepairStateFactory.create(tableReference, repairConfiguration, postUpdateHook);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Repair history lookback time", description = "The lookback time in seconds for when the repair_history table is queried to get initial repair state at startup")
        long lookbackTimeSeconds() default DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS;
    }
}
