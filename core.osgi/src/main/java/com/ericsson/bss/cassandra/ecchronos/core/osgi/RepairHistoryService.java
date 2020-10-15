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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import java.util.Iterator;

import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;

@Component(service = RepairHistoryProvider.class)
@Designate(ocd = RepairHistoryService.Configuration.class)
public class RepairHistoryService implements RepairHistoryProvider
{
    private static final long DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS = 30L * 24L * 60L * 60L;

    @Reference(service = StatementDecorator.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile StatementDecorator statementDecorator;

    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider nativeConnectionProvider;

    @Reference(service = NodeResolver.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NodeResolver nodeResolver;

    private volatile RepairHistoryProvider delegateRepairHistoryProvider;

    @Activate
    public void activate(Configuration configuration)
    {
        long lookbackTimeInMillis = configuration.lookbackTimeSeconds() * 1000;
        delegateRepairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver,
                nativeConnectionProvider.getSession(), statementDecorator, lookbackTimeInMillis);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to,
            Predicate<RepairEntry> predicate)
    {
        return delegateRepairHistoryProvider.iterate(tableReference, to, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from,
            Predicate<RepairEntry> predicate)
    {
        return delegateRepairHistoryProvider.iterate(tableReference, to, from, predicate);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Repair history lookback time", description = "The lookback time in seconds for when the repair_history table is queried to get initial repair state at startup")
        long lookbackTimeSeconds() default DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS;
    }
}
