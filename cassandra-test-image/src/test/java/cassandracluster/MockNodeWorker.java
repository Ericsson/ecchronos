/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

package cassandracluster;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.KeyspaceCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.multithread.TableCreatedEvent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.Set;
import java.util.function.Function;

public class MockNodeWorker extends NodeWorker {

    Integer tableCreateCount = 0;
    Integer tableRemoveCount = 0;
    Integer keyspaceCreateCount = 0;


    public MockNodeWorker(
            final Node node,
            final ReplicatedTableProvider replicatedTableProvider,
            final RepairScheduler repairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final Function<TableReference, Set<RepairConfiguration>> repairConfigurationFunction,
            final CqlSession session)
    {
        super (node,replicatedTableProvider, repairScheduler, tableReferenceFactory, repairConfigurationFunction, session);
    }
    @Override
    protected void onTableCreated(final TableCreatedEvent tableEvent)
    {
        tableCreateCount ++;
        super.onTableCreated(tableEvent);


    }
    @Override
    protected void removeConfiguration(final TableMetadata table){
        tableRemoveCount++;
        super.removeConfiguration(table);
    }

    @Override
    protected void onKeyspaceCreated(final KeyspaceCreatedEvent keyspaceEvent)
    {
        keyspaceCreateCount++;
        super.onKeyspaceCreated(keyspaceEvent);
    }
    public Integer getTableCreateCount() {
        return tableCreateCount;
    }
    public Integer getTableRemoveCount() {
        return tableRemoveCount;
    }

    public Integer getKeyspaceCreateCount() {
        return keyspaceCreateCount;
    }

}
