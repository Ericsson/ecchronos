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
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import java.util.Map;

@Component(service = ReplicationState.class)
public class ReplicationStateService implements ReplicationState
{
    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider nativeConnectionProvider;

    @Reference(service = NodeResolver.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NodeResolver nodeResolver;

    private volatile ReplicationState delegateReplicationState;

    @Activate
    public void activate()
    {
        CqlSession session = nativeConnectionProvider.getSession();
        com.datastax.oss.driver.api.core.metadata.Node localNode = nativeConnectionProvider.getLocalNode();

        delegateReplicationState = new ReplicationStateImpl(nodeResolver, session, localNode);
    }

    @Override
    public ImmutableSet<Node> getNodes(TableReference tableReference, LongTokenRange tokenRange)
    {
        return delegateReplicationState.getNodes(tableReference, tokenRange);
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<Node>> getTokenRangeToReplicas(TableReference tableReference)
    {
        return delegateReplicationState.getTokenRangeToReplicas(tableReference);
    }

    @Override
    public Map<LongTokenRange, ImmutableSet<Node>> getTokenRanges(TableReference tableReference)
    {
        return delegateReplicationState.getTokenRanges(tableReference);
    }
}
