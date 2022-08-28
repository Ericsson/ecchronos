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
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import java.util.Set;

@Component(service = TableReferenceFactory.class)
public class TableReferenceFactoryService implements TableReferenceFactory
{
    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider nativeConnectionProvider;

    private volatile TableReferenceFactory delegateTableReferenceFactory;

    @Activate
    public void activate()
    {
        CqlSession session = nativeConnectionProvider.getSession();

        delegateTableReferenceFactory = new TableReferenceFactoryImpl(session);
    }

    @Override
    public TableReference forTable(String keyspace, String table)
    {
        return delegateTableReferenceFactory.forTable(keyspace, table);
    }

    @Override
    public TableReference forTable(TableMetadata table)
    {
        return delegateTableReferenceFactory.forTable(table);
    }

    @Override
    public Set<TableReference> forKeyspace(String keyspace) throws EcChronosException
    {
        return delegateTableReferenceFactory.forKeyspace(keyspace);
    }

    @Override
    public Set<TableReference> forCluster()
    {
        return delegateTableReferenceFactory.forCluster();
    }
}
