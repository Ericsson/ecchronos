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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import java.util.Set;

@Component(service = ReplicatedTableProvider.class)
public class ReplicatedTableProviderService implements ReplicatedTableProvider
{
    private volatile ReplicatedTableProvider myDelegateReplicatedTableProvider;

    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider myNativeConnectionProvider;

    @Reference(service = TableReferenceFactory.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile TableReferenceFactory myTableReferenceFactory;

    @Activate
    public void activate()
    {
        CqlSession session = myNativeConnectionProvider.getSession();
        Node localhost = myNativeConnectionProvider.getLocalNode();

        myDelegateReplicatedTableProvider = new ReplicatedTableProviderImpl(localhost, session,
                myTableReferenceFactory);
    }

    @Override
    public Set<TableReference> getAll()
    {
        return myDelegateReplicatedTableProvider.getAll();
    }

    @Override
    public boolean accept(String keyspace)
    {
        return myDelegateReplicatedTableProvider.accept(keyspace);
    }
}
