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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.osgi.service.component.annotations.*;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;

@Component(service = { RepairHistory.class, RepairHistoryProvider.class })
@Designate(ocd = RepairHistoryService.Configuration.class)
public class RepairHistoryService implements RepairHistory, RepairHistoryProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairHistoryService.class);

    private static final long DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS = 30L * 24L * 60L * 60L;

    public enum Provider
    {
        CASSANDRA,
        ECC
    }

    @Reference(service = StatementDecorator.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile StatementDecorator statementDecorator;

    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider nativeConnectionProvider;

    @Reference(service = NodeResolver.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NodeResolver nodeResolver;

    @Reference(service = ReplicationState.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ReplicationState replicationState;

    private volatile EccRepairHistory delegateRepairHistory;

    private volatile RepairHistoryProvider delegateRepairHistoryProvider;

    @Activate
    public void activate(Configuration configuration)
    {
        Host localHost = nativeConnectionProvider.getLocalHost();
        Optional<Node> localNode = nodeResolver.fromUUID(localHost.getHostId());
        if (!localNode.isPresent())
        {
            LOG.error("Local node ({}) not found in resolver", localHost.getHostId());
            throw new IllegalStateException("Local node (" + localHost.getHostId() + ") not found in resolver");
        }

        long lookbackTimeInMillis = configuration.lookbackTimeSeconds() * 1000;
        delegateRepairHistory = EccRepairHistory.newBuilder()
                .withLocalNode(localNode.get())
                .withReplicationState(replicationState)
                .withSession(nativeConnectionProvider.getSession())
                .withStatementDecorator(statementDecorator)
                .withLookbackTime(lookbackTimeInMillis, TimeUnit.MILLISECONDS)
                .build();

        switch (configuration.provider())
        {
        case ECC:
            delegateRepairHistoryProvider = delegateRepairHistory;
            break;
        case CASSANDRA:
        default:
            delegateRepairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver,
                    nativeConnectionProvider.getSession(), statementDecorator, lookbackTimeInMillis);
            break;
        }
    }

    @Override
    public RepairSession newSession(TableReference tableReference, UUID jobId, LongTokenRange range,
            Set<Node> participants)
    {
        return delegateRepairHistory.newSession(tableReference, jobId, range, participants);
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
        @AttributeDefinition(name = "The provider to use for repair history", description = "The provider to use for repair history")
        Provider provider() default Provider.ECC;

        @AttributeDefinition(name = "Repair history lookback time", description = "The lookback time in seconds for when the repair_history table is queried to get initial repair state at startup")
        long lookbackTimeSeconds() default DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS;
    }
}
