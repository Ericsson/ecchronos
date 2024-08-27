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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.EccRepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component(service = { RepairHistory.class, RepairHistoryProvider.class })
@Designate(ocd = RepairHistoryService.Configuration.class)
public class RepairHistoryService implements RepairHistory, RepairHistoryProvider
{
    private static final int ONE_SECOND_MILLIS = 1000;
    private static final Logger LOG = LoggerFactory.getLogger(RepairHistoryService.class);

    private static final String DEFAULT_PROVIDER_KEYSPACE = "ecchronos";

    private static final long DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS = 30L * 24L * 60L * 60L;

    public enum Provider
    {
        CASSANDRA,
        UPGRADE,
        ECC
    }

    @Reference(service = StatementDecorator.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile StatementDecorator statementDecorator;

    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider nativeConnectionProvider;

    @Reference(service = NodeResolver.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NodeResolver nodeResolver;

    @Reference(service = ReplicationState.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile ReplicationState replicationState;

    private volatile RepairHistory delegateRepairHistory;

    private volatile RepairHistoryProvider delegateRepairHistoryProvider;

    @Activate
    public final void activate(final Configuration configuration)
    {
        Node node = nativeConnectionProvider.getLocalNode();
        Optional<DriverNode> localNode = nodeResolver.fromUUID(node.getHostId());
        if (!localNode.isPresent())
        {
            String msg = String.format("Local node (%s) not found in resolver", node.getHostId());
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        long lookbackTimeInMillis = configuration.lookbackTimeSeconds() * ONE_SECOND_MILLIS;

        CqlSession session = nativeConnectionProvider.getSession();

        if (configuration.provider() == Provider.CASSANDRA)
        {
            delegateRepairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver,
                    session, statementDecorator,
                    lookbackTimeInMillis);
            delegateRepairHistory = RepairHistory.NO_OP;
        }
        else
        {
            EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                    .withLocalNode(localNode.get())
                    .withReplicationState(replicationState)
                    .withSession(nativeConnectionProvider.getSession())
                    .withStatementDecorator(statementDecorator)
                    .withLookbackTime(lookbackTimeInMillis, TimeUnit.MILLISECONDS)
                    .build();

            if (configuration.provider() == Provider.UPGRADE)
            {
                delegateRepairHistoryProvider = new RepairHistoryProviderImpl(nodeResolver, session, statementDecorator,
                        lookbackTimeInMillis);
            }
            else
            {
                delegateRepairHistoryProvider = eccRepairHistory;
            }

            delegateRepairHistory = eccRepairHistory;
        }
    }

    @Override
    public final RepairSession newSession(final TableReference tableReference,
                                          final UUID jobId,
                                          final LongTokenRange range,
                                          final Set<DriverNode> participants)
    {
        return delegateRepairHistory.newSession(tableReference, jobId, range, participants);
    }

    @Override
    public final Iterator<RepairEntry> iterate(final TableReference tableReference,
                                               final long to,
                                               final Predicate<RepairEntry> predicate)
    {
        return delegateRepairHistoryProvider.iterate(tableReference, to, predicate);
    }

    @Override
    public final Iterator<RepairEntry> iterate(final TableReference tableReference,
                                               final long to,
                                               final long from,
                                               final Predicate<RepairEntry> predicate)
    {
        return delegateRepairHistoryProvider.iterate(tableReference, to, from, predicate);
    }

    @Override
    public final Iterator<RepairEntry> iterate(final UUID nodeId,
                                               final TableReference tableReference,
                                               final long to,
                                               final long from,
                                               final Predicate<RepairEntry> predicate)
    {
        return delegateRepairHistoryProvider.iterate(nodeId, tableReference, to, from, predicate);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "The provider to use for repair history",
                description = "The provider to use for repair history")
        Provider provider() default Provider.ECC;

        @AttributeDefinition(name = "Provider keyspace", description = "The keyspace used for ecc history if enabled")
        String providerKeyspace() default DEFAULT_PROVIDER_KEYSPACE;

        @AttributeDefinition(name = "Repair history lookback time",
                description = "The lookback time in seconds for when the repair_history table is queried to get "
                            + "initial repair state at startup")
        long lookbackTimeSeconds() default DEFAULT_REPAIR_HISTORY_LOOKBACK_SECONDS;
    }
}
